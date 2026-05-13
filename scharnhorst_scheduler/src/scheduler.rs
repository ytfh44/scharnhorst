use std::collections::{HashMap, VecDeque};
use std::sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Mutex};

use scharnhorst_core::Tick;
use scharnhorst_journal::command::CommandEnvelope;
use scharnhorst_journal::commit::CommitResult;
use scharnhorst_journal::journal::Journal;
use scharnhorst_query::engine::QueryEngine;

use crate::error::{SchedulerError, SchedulerResult};
use crate::phase::Phase;
use crate::refresh_signal::{RefreshSignalBus, RefreshSignalHandle};
use crate::rng::DeterministicRng;
use crate::system::{BoxedSystem, SystemRegistration};

/// The central simulation scheduler.
///
/// Responsibilities:
/// - Phase-based execution of registered [`SimSystem`]s.
/// - Tick-boundary command consumption from the bridge.
/// - Read-Snapshot -> Write-Journal -> Atomic Commit cycle.
/// - Deterministic RNG stream provisioning per system/tick.
/// - System dependency registration via query-engine.
/// - Snapshot refresh signal protocol for consumers.
pub struct Scheduler {
 /// Registered simulation systems keyed by ID.
    systems: Arc<Mutex<HashMap<String, BoxedSystem>>>,
 /// Cached system registrations for conflict detection.
    registrations: Arc<Mutex<HashMap<String, SystemRegistration>>>,
 /// The command queue fed by the bevy-bridge input buffer.
    pending_commands: Arc<Mutex<VecDeque<CommandEnvelope>>>,
 /// The deterministic journal for atomic commit.
    journal: Arc<Mutex<Journal>>,
 /// Read-only query engine (unified read interface).
    query_engine: Arc<QueryEngine>,
 /// Refresh signal bus for snapshot generation protocol.
    refresh_bus: RefreshSignalBus,
 /// Current simulation tick.
    current_tick: Arc<AtomicU64>,
 /// Current snapshot generation (monotonically increasing).
    generation: Arc<AtomicU64>,
 /// Whether the scheduler has been initialized.
    initialized: Arc<AtomicBool>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sys_count = self
            .systems
            .lock()
            .map(|m| m.len())
            .unwrap_or(0);
        let cmd_count = self
            .pending_commands
            .lock()
            .map(|m| m.len())
            .unwrap_or(0);
        let tick = Tick(self.current_tick.load(Ordering::Relaxed));
        let gen = self.generation.load(Ordering::Relaxed);
        f.debug_struct("Scheduler")
            .field("system_count", &sys_count)
            .field("pending_commands", &cmd_count)
            .field("current_tick", &tick)
            .field("generation", &gen)
            .field("refresh_bus", &self.refresh_bus)
            .finish()
    }
}

impl Scheduler {
 /// Create a new scheduler with the given journal and query engine.
    pub fn new(journal: Journal, query_engine: QueryEngine) -> Self {
        Self {
            systems: Arc::new(Mutex::new(HashMap::new())),
            registrations: Arc::new(Mutex::new(HashMap::new())),
            pending_commands: Arc::new(Mutex::new(VecDeque::new())),
            journal: Arc::new(Mutex::new(journal)),
            query_engine: Arc::new(query_engine),
            refresh_bus: RefreshSignalBus::new(),
            current_tick: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(AtomicU64::new(0)),
            initialized: Arc::new(AtomicBool::new(false)),
        }
    }

 // ------------------------------------------------------------------
 // System registration
 // ------------------------------------------------------------------

 /// Register a simulation system.
 ///
 /// Returns an error if a system with the same ID is already registered
 /// or if the registration introduces a write conflict within a phase.
    pub fn register_system(&self, system: BoxedSystem) -> SchedulerResult<()> {
        let id = system.id().to_owned();
        let reg = SystemRegistration::from_system(system.as_ref());

        let mut systems = self.lock_systems()?;
        let mut registrations = self.lock_registrations()?;

        if systems.contains_key(&id) {
            return Err(SchedulerError::SystemAlreadyRegistered(id));
        }

 // Detect write conflicts with existing systems in the same phase.
        let conflicts: Vec<_> = registrations
            .values()
            .filter(|other| reg.has_write_conflict(other))
            .collect();

        if let Some(conflict) = conflicts.first() {
            let tables = reg.conflicting_tables(conflict);
            let table = tables.into_iter().next().unwrap_or_default();
            return Err(SchedulerError::WriteConflict {
                phase: format!("{:?}", reg.phase),
                a: id.clone(),
                b: conflict.system_id.clone(),
                table,
            });
        }

 // Register refresh callback if present.
        if let Some(cb) = &reg.refresh_callback {
 // Handle is discarded intentionally 鈥?RefreshSignalBus stores callbacks by name,
 // not through the handle. Dropping the handle does NOT unregister.
            let _handle = self.refresh_bus.register(id.clone(), cb.clone())?;
        }

        registrations.insert(id.clone(), reg);
        systems.insert(id, system);
        Ok(())
    }

 /// Unregister a system by ID.
    pub fn unregister_system(&self, system_id: &str) -> SchedulerResult<()> {
        let mut systems = self.lock_systems()?;
        let mut registrations = self.lock_registrations()?;
        systems.remove(system_id);
        registrations.remove(system_id);
        Ok(())
    }

 /// Returns the IDs of all registered systems.
    pub fn system_ids(&self) -> SchedulerResult<Vec<String>> {
        let systems = self.lock_systems()?;
        Ok(systems.keys().cloned().collect())
    }

 /// Returns the registration for a given system ID.
    pub fn registration(&self, system_id: &str) -> SchedulerResult<SystemRegistration> {
        let registrations = self.lock_registrations()?;
        registrations
            .get(system_id)
            .cloned()
            .ok_or_else(|| SchedulerError::SystemNotFound(system_id.to_owned()))
    }

 /// Returns all system IDs grouped by phase, in phase order.
    pub fn systems_by_phase(&self) -> SchedulerResult<Vec<(Phase, Vec<String>)>> {
        let registrations = self.lock_registrations()?;
        let mut map: HashMap<Phase, Vec<String>> = HashMap::new();
        for (id, reg) in registrations.iter() {
            map.entry(reg.phase).or_default().push(id.clone());
        }
        let mut pairs: Vec<_> = map.into_iter().collect();
        pairs.sort_by_key(|(phase, _)| *phase);
        Ok(pairs)
    }

 // ------------------------------------------------------------------
 // Command buffer (tick-boundary consumption, )
 // ------------------------------------------------------------------

 /// Enqueue a command envelope (called by the bevy-bridge input buffer).
    pub fn enqueue_command(&self, envelope: CommandEnvelope) -> SchedulerResult<()> {
        let mut queue = self.lock_commands()?;
        queue.push_back(envelope);
        Ok(())
    }

 /// Drain all pending commands and submit them to the journal.
 ///
 /// This is called at the tick boundary before any simulation phase runs.
    pub fn consume_pending_commands(&self) -> SchedulerResult<Vec<CommandEnvelope>> {
        let mut queue = self.lock_commands()?;
        let drained: Vec<_> = queue.drain(..).collect();
        let mut journal = self.lock_journal()?;
        for env in &drained {
            journal.submit_command(env.clone())?;
        }
        Ok(drained)
    }

 /// Returns the number of pending commands.
    pub fn pending_command_count(&self) -> SchedulerResult<usize> {
        let queue = self.lock_commands()?;
        Ok(queue.len())
    }

 // ------------------------------------------------------------------
 // Tick lifecycle
 // ------------------------------------------------------------------

 /// Advance the simulation by one tick.
 ///
 /// 1. Consume pending commands at tick boundary.
 /// 2. Execute all phases in order.
 /// 3. Trigger atomic commit.
 /// 4. Broadcast refresh signal.
    pub fn tick(&self) -> SchedulerResult<CommitResult> {
        let tick = self.current_tick()?;
        self.consume_pending_commands()?;
        self.run_phases(tick)?;
        let result = self.atomic_commit()?;
        self.broadcast_refresh(tick, result.state_hash)?;
        self.advance_tick()?;
        Ok(result)
    }

 /// Run all simulation phases for the current tick.
    pub fn run_phases(&self, tick: Tick) -> SchedulerResult<()> {
        let by_phase = self.systems_by_phase()?;
        for (phase, ids) in by_phase {
            self.run_phase(phase, &ids, tick)?;
        }
        Ok(())
    }

 /// Run a single phase.
 ///
 /// Systems within the same phase are executed sequentially in this stub.
 /// A full implementation may parallelize when write sets are disjoint.
    pub fn run_phase(&self, phase: Phase, system_ids: &[String], tick: Tick) -> SchedulerResult<()> {
        let systems = self.lock_systems()?;
        for id in system_ids {
            let system = systems
                .get(id)
                .ok_or_else(|| SchedulerError::SystemNotFound(id.clone()))?;
            let mut rng = DeterministicRng::new(id.clone(), tick.as_u64());
            let diffs = system.execute(&mut rng, &self.query_engine, phase, tick.as_u64())?;
            let mut journal = self.lock_journal()?;
            for diff in diffs {
                journal.submit_diff(diff)?;
            }
        }
        Ok(())
    }

 /// Perform the atomic commit at the end of the tick.
 ///
 /// Read-Snapshot -> Write-Journal -> Atomic Commit cycle.
    pub fn atomic_commit(&self) -> SchedulerResult<CommitResult> {
        let mut journal = self.lock_journal()?;
        let result = journal.commit()?;
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

 /// Broadcast the refresh signal to all registered consumers.
    pub fn broadcast_refresh(&self, tick: Tick, state_hash: u64) -> SchedulerResult<()> {
        self.refresh_bus.broadcast(tick.as_u64(), state_hash)
    }

 /// Advance the internal tick counter.
    pub fn advance_tick(&self) -> SchedulerResult<Tick> {
        let prev = self.current_tick.fetch_add(1, Ordering::Relaxed);
        Ok(Tick(prev).next())
    }

 /// Returns the current tick.
    pub fn current_tick(&self) -> SchedulerResult<Tick> {
        let raw = self.current_tick.load(Ordering::Relaxed);
        Ok(Tick(raw))
    }

 /// Returns the current snapshot generation.
    pub fn current_generation(&self) -> SchedulerResult<u64> {
        Ok(self.generation.load(Ordering::Relaxed))
    }

 // ------------------------------------------------------------------
 // Refresh signal consumer registration
 // ------------------------------------------------------------------

 /// Register an external consumer for refresh signals.
    pub fn register_consumer(
        &self,
        name: impl Into<String>,
        callback: crate::refresh_signal::RefreshCallback,
    ) -> SchedulerResult<RefreshSignalHandle> {
        self.refresh_bus.register(name, callback)
    }

 /// Unregister an external consumer.
    pub fn unregister_consumer(&self, handle: &RefreshSignalHandle) -> SchedulerResult<()> {
        self.refresh_bus.unregister(handle)
    }

 /// Returns the names of all registered refresh-signal consumers.
    pub fn consumer_names(&self) -> SchedulerResult<Vec<String>> {
        self.refresh_bus.consumer_names()
    }

 // ------------------------------------------------------------------
 // Initialization
 // ------------------------------------------------------------------

 /// Initialize the scheduler: validate registrations before first tick.
 ///
 /// Validates: (1) systems are registered, (2) each declares read/write tables,
 /// (3) registrations match system map, (4) no write conflicts within same phase.
 /// Safe to call multiple times 鈥?subsequent calls are no-ops.
    pub fn initialize(&self) -> SchedulerResult<()> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }

 // Lock in established order (systems first, then registrations)
 // to maintain consistent lock ordering and prevent deadlock.
        let systems = self.lock_systems()?;
        let registrations = self.lock_registrations()?;

        for (id, reg) in registrations.iter() {
            if reg.read_tables.is_empty() && reg.write_tables.is_empty() {
                return Err(SchedulerError::Generic(format!(
                    "system '{id}' has no read or write tables declared"
                )));
            }
        }

        for id in registrations.keys() {
            if !systems.contains_key(id) {
                return Err(SchedulerError::Generic(format!(
                    "registration '{id}' has no corresponding system"
                )));
            }
        }

        let reg_refs: Vec<&SystemRegistration> = registrations.values().collect();
        for i in 0..reg_refs.len() {
            for j in (i + 1)..reg_refs.len() {
                if reg_refs[i].has_write_conflict(reg_refs[j]) {
                    let conflicting = reg_refs[i].conflicting_tables(reg_refs[j]);
                    if let Some(table) = conflicting.into_iter().next() {
                        return Err(SchedulerError::WriteConflict {
                            phase: format!("{:?}", reg_refs[i].phase),
                            a: reg_refs[i].system_id.clone(),
                            b: reg_refs[j].system_id.clone(),
                            table,
                        });
                    }
                }
            }
        }

        for _reg in registrations.values() {
            let _phase_order = _reg.phase.order_index();
        }

        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

 /// Returns true if the scheduler has been initialized.
    pub fn is_initialized(&self) -> SchedulerResult<bool> {
        Ok(self.initialized.load(Ordering::Acquire))
    }

 // ------------------------------------------------------------------
 // Lock helpers (avoid unwrap/expect)
 // ------------------------------------------------------------------

    fn lock_systems(&self) -> SchedulerResult<std::sync::MutexGuard<'_, HashMap<String, BoxedSystem>>> {
        self.systems
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("systems lock poisoned: {e}")))
    }

    fn lock_registrations(
        &self,
    ) -> SchedulerResult<std::sync::MutexGuard<'_, HashMap<String, SystemRegistration>>> {
        self.registrations
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("registrations lock poisoned: {e}")))
    }

    fn lock_commands(&self) -> SchedulerResult<std::sync::MutexGuard<'_, VecDeque<CommandEnvelope>>> {
        self.pending_commands
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("commands lock poisoned: {e}")))
    }

    fn lock_journal(&self) -> SchedulerResult<std::sync::MutexGuard<'_, Journal>> {
        self.journal
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("journal lock poisoned: {e}")))
    }

 // ------------------------------------------------------------------
 // Store and engine attachment (runtime replacement)
 // ------------------------------------------------------------------

 /// Replace the query engine with a new instance.
    pub fn attach_query_engine(&mut self, engine: QueryEngine) {
        self.query_engine = Arc::new(engine);
    }

 /// Access the journal via a locked guard.
    pub fn journal_mut(&self) -> SchedulerResult<std::sync::MutexGuard<'_, Journal>> {
        self.lock_journal()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use scharnhorst_core::Tick;
    use scharnhorst_journal::command::{Command, CommandEnvelope};
    use scharnhorst_journal::diff::Diff;
    use scharnhorst_journal::journal::Journal;
    use scharnhorst_query::engine::QueryEngine;
    use scharnhorst_schema::SchemaRegistry;

    use super::*;
    use crate::error::SchedulerResult;
    use crate::phase::Phase;
    use crate::rng::DeterministicRng;
    use crate::system::SimSystem;

    fn make_scheduler() -> Scheduler {
        Scheduler::new(Journal::default(), QueryEngine::new(SchemaRegistry::new()))
    }

    #[test]
    fn new_creates_at_tick_zero() {
        let s = make_scheduler();
        assert_eq!(s.current_tick().unwrap(), Tick::ZERO);
        assert_eq!(s.current_generation().unwrap(), 0);
    }

    #[test]
    fn register_and_unregister_system() {
        let s = make_scheduler();
        struct S;
        impl SimSystem for S {
            fn id(&self) -> &str { "s" }
            fn phase(&self) -> Phase { Phase::Economy }
            fn read_tables(&self) -> Vec<String> { vec![] }
            fn write_tables(&self) -> Vec<String> { vec![] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(S)).unwrap();
        assert_eq!(s.system_ids().unwrap().len(), 1);
        s.unregister_system("s").unwrap();
        assert_eq!(s.system_ids().unwrap().len(), 0);
    }

    #[test]
    fn duplicate_register_fails() {
        let s = make_scheduler();
        struct S;
        impl SimSystem for S {
            fn id(&self) -> &str { "dup" }
            fn phase(&self) -> Phase { Phase::Economy }
            fn read_tables(&self) -> Vec<String> { vec![] }
            fn write_tables(&self) -> Vec<String> { vec![] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(S)).unwrap();
        let err = s.register_system(Arc::new(S)).unwrap_err();
        assert!(matches!(err, SchedulerError::SystemAlreadyRegistered(ref id) if id == "dup"));
    }

    #[test]
    fn enqueue_and_consume_commands() {
        let s = make_scheduler();
        let env = CommandEnvelope::new(Tick::ZERO, "test", Command::Raw {
            domain: "move".into(),
            payload: serde_json::json!({}),
        });
        s.enqueue_command(env).unwrap();
        assert_eq!(s.pending_command_count().unwrap(), 1);
        let consumed = s.consume_pending_commands().unwrap();
        assert_eq!(consumed.len(), 1);
        assert_eq!(s.pending_command_count().unwrap(), 0);
    }

    #[test]
    fn tick_advances_counter() {
        let s = make_scheduler();
        assert_eq!(s.current_tick().unwrap(), Tick::ZERO);
        s.tick().unwrap();
        assert_eq!(s.current_tick().unwrap(), Tick(1));
        s.tick().unwrap();
        assert_eq!(s.current_tick().unwrap(), Tick(2));
    }

    #[test]
    fn atomic_commit_advances_generation() {
        let s = make_scheduler();
        let before = s.current_generation().unwrap();
        s.atomic_commit().unwrap();
        assert_eq!(s.current_generation().unwrap(), before + 1);
    }

    #[test]
    fn initialize_is_idempotent() {
        let s = make_scheduler();
        struct S;
        impl SimSystem for S {
            fn id(&self) -> &str { "s" }
            fn phase(&self) -> Phase { Phase::Economy }
            fn read_tables(&self) -> Vec<String> { vec!["a".into()] }
            fn write_tables(&self) -> Vec<String> { vec![] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(S)).unwrap();
        assert!(!s.is_initialized().unwrap());
        s.initialize().unwrap();
        assert!(s.is_initialized().unwrap());
        s.initialize().unwrap();
        assert!(s.is_initialized().unwrap());
    }

    #[test]
    fn initialize_succeeds_with_no_systems() {
        let s = make_scheduler();
        s.initialize().unwrap();
        assert!(s.is_initialized().unwrap());
    }

    #[test]
    fn initialize_fails_when_system_has_no_tables() {
        let s = make_scheduler();
        struct S;
        impl SimSystem for S {
            fn id(&self) -> &str { "bare" }
            fn phase(&self) -> Phase { Phase::Economy }
            fn read_tables(&self) -> Vec<String> { vec![] }
            fn write_tables(&self) -> Vec<String> { vec![] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(S)).unwrap();
        let err = s.initialize().unwrap_err();
        assert!(matches!(err, SchedulerError::Generic(ref msg) if msg.contains("has no read or write tables")));
    }

    #[test]
    fn initialize_succeeds_with_valid_systems() {
        let s = make_scheduler();
        struct A;
        impl SimSystem for A {
            fn id(&self) -> &str { "a" }
            fn phase(&self) -> Phase { Phase::Economy }
            fn read_tables(&self) -> Vec<String> { vec!["trade".into()] }
            fn write_tables(&self) -> Vec<String> { vec!["prices".into()] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        struct B;
        impl SimSystem for B {
            fn id(&self) -> &str { "b" }
            fn phase(&self) -> Phase { Phase::Diplomacy }
            fn read_tables(&self) -> Vec<String> { vec!["prices".into()] }
            fn write_tables(&self) -> Vec<String> { vec!["treaties".into()] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(A)).unwrap();
        s.register_system(Arc::new(B)).unwrap();
        s.initialize().unwrap();
        assert!(s.is_initialized().unwrap());
    }

    #[test]
    fn attach_query_engine_updates_engine() {
        let mut s = make_scheduler();
        let new_engine = QueryEngine::new(SchemaRegistry::new());
        s.attach_query_engine(new_engine);
    }

    #[test]
    fn journal_mut_provides_lock_guard() {
        let s = make_scheduler();
        let guard = s.journal_mut().unwrap();
        assert_eq!(guard.current_tick(), Tick::ZERO);
        drop(guard);
    }

    #[test]
    fn systems_by_phase_groups_correctly() {
        let s = make_scheduler();
        struct Sys { id: &'static str, phase: Phase }
        impl SimSystem for Sys {
            fn id(&self) -> &str { self.id }
            fn phase(&self) -> Phase { self.phase }
            fn read_tables(&self) -> Vec<String> { vec![] }
            fn write_tables(&self) -> Vec<String> { vec![] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(Sys { id: "pre", phase: Phase::PreTick })).unwrap();
        s.register_system(Arc::new(Sys { id: "post", phase: Phase::PostTick })).unwrap();
        let by_phase = s.systems_by_phase().unwrap();
        assert_eq!(by_phase.len(), 2);
        assert_eq!(by_phase[0].0, Phase::PreTick);
        assert_eq!(by_phase[0].1, vec!["pre"]);
        assert_eq!(by_phase[1].0, Phase::PostTick);
        assert_eq!(by_phase[1].1, vec!["post"]);
    }

    #[test]
    fn registration_returns_metadata() {
        let s = make_scheduler();
        struct Sys;
        impl SimSystem for Sys {
            fn id(&self) -> &str { "meta" }
            fn phase(&self) -> Phase { Phase::Military }
            fn read_tables(&self) -> Vec<String> { vec!["units".into()] }
            fn write_tables(&self) -> Vec<String> { vec!["battles".into()] }
            fn execute(&self, _: &mut DeterministicRng, _: &QueryEngine, _: Phase, _: u64) -> SchedulerResult<Vec<Diff>> {
                Ok(vec![])
            }
        }
        s.register_system(Arc::new(Sys)).unwrap();
        let reg = s.registration("meta").unwrap();
        assert_eq!(reg.system_id, "meta");
        assert_eq!(reg.phase, Phase::Military);
    }

    #[test]
    fn registration_missing_returns_error() {
        let s = make_scheduler();
        let err = s.registration("nope").unwrap_err();
        assert!(matches!(err, SchedulerError::SystemNotFound(ref id) if id == "nope"));
    }
}
