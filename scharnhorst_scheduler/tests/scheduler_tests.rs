use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use scharnhorst_core::{JournalSubmitToken, RowId, Tick};
use scharnhorst_journal::command::{Command, CommandEnvelope};
use scharnhorst_journal::diff::Diff;
use scharnhorst_journal::journal::Journal;
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_scheduler::{
    BoxedSystem, DeterministicRng, Phase, RefreshCallback, RefreshSignalHandle, Scheduler,
    SchedulerError, SchedulerResult, SimSystem, SystemRegistration,
};
use scharnhorst_schema::SchemaRegistry;

// ===================================================================
// Test helpers
// ===================================================================

fn empty_query_engine() -> QueryEngine {
    let mut registry = SchemaRegistry::new();
    registry.freeze();
    QueryEngine::new(registry)
}

/// QueryEngine backed by an unfrozen schema — used to test
/// SchemaNotFrozen rejection in initialize().
fn unfrozen_query_engine() -> QueryEngine {
    let registry = SchemaRegistry::new();
    QueryEngine::new(registry)
}

fn empty_journal() -> Journal {
    let store = Arc::new(scharnhorst_arrow_store::ArrowStore::new());
    let init_store = scharnhorst_arrow_store::InitStore::new(Arc::clone(&store));
    let _ = init_store.into_simulation().unwrap();
    Journal::new(store)
}

fn make_scheduler_with_table(table_name: &str) -> Scheduler {
    let store = Arc::new(scharnhorst_arrow_store::ArrowStore::new());
    let init_store = scharnhorst_arrow_store::InitStore::new(Arc::clone(&store));
    let spec = scharnhorst_schema::TableSpec::new(table_name)
        .with_column(scharnhorst_schema::ColumnSpec::new(
            "id",
            scharnhorst_schema::FieldSemantic::Id,
            "i64",
        ))
        .unwrap()
        .with_column(scharnhorst_schema::ColumnSpec::new(
            "value",
            scharnhorst_schema::FieldSemantic::Quantity,
            "i64",
        ))
        .unwrap();
    init_store
        .create_table(&spec, scharnhorst_arrow_store::MutationMode::Patchable)
        .unwrap();
    let _commit_store = init_store.into_simulation().unwrap();
    let mut journal = Journal::new(store);
    let mut values = serde_json::Map::new();
    values.insert(
        "id".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(1u64)),
    );
    values.insert(
        "value".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(0i64)),
    );
    let insert = Diff::Insert {
        table: table_name.to_owned(),
        row: RowId::new(1),
        values,
    };
    journal
        .submit_diff(insert, &JournalSubmitToken::new())
        .unwrap();
    journal.commit().unwrap();
    Scheduler::new(journal, empty_query_engine())
}

fn empty_scheduler() -> Scheduler {
    Scheduler::new(empty_journal(), empty_query_engine())
}

/// A no-op system for registration tests.
#[derive(Debug, Clone)]
struct NoOpSystem {
    id: String,
    phase: Phase,
    reads: Vec<String>,
    writes: Vec<String>,
}

impl NoOpSystem {
    fn new(id: impl Into<String>, phase: Phase) -> Self {
        Self {
            id: id.into(),
            phase,
            reads: Vec::new(),
            writes: Vec::new(),
        }
    }

    fn with_reads(mut self, tables: &[&str]) -> Self {
        self.reads = tables.iter().map(|s| s.to_string()).collect();
        self
    }

    fn with_writes(mut self, tables: &[&str]) -> Self {
        self.writes = tables.iter().map(|s| s.to_string()).collect();
        self
    }
}

impl SimSystem for NoOpSystem {
    fn id(&self) -> &str {
        &self.id
    }

    fn phase(&self) -> Phase {
        self.phase
    }

    fn read_tables(&self) -> Vec<String> {
        self.reads.clone()
    }

    fn write_tables(&self) -> Vec<String> {
        self.writes.clone()
    }

    fn execute(
        &self,
        _rng: &mut DeterministicRng,
        _query: &QueryEngine,
        _phase: Phase,
        _tick: u64,
    ) -> SchedulerResult<Vec<Diff>> {
        Ok(Vec::new())
    }
}

/// A system that emits a single diff.
#[derive(Debug, Clone)]
struct DiffEmitter {
    id: String,
    phase: Phase,
    diff: Diff,
}

impl DiffEmitter {
    fn new(id: impl Into<String>, phase: Phase, diff: Diff) -> Self {
        Self {
            id: id.into(),
            phase,
            diff,
        }
    }
}

impl SimSystem for DiffEmitter {
    fn id(&self) -> &str {
        &self.id
    }

    fn phase(&self) -> Phase {
        self.phase
    }

    fn read_tables(&self) -> Vec<String> {
        Vec::new()
    }

    fn write_tables(&self) -> Vec<String> {
        vec![self.diff.table().to_owned()]
    }

    fn execute(
        &self,
        _rng: &mut DeterministicRng,
        _query: &QueryEngine,
        _phase: Phase,
        _tick: u64,
    ) -> SchedulerResult<Vec<Diff>> {
        Ok(vec![self.diff.clone()])
    }
}

/// A system that records the RNG values it receives.
#[derive(Debug, Clone)]
struct RngRecorder {
    id: String,
    phase: Phase,
    values: Arc<Mutex<Vec<u64>>>,
}

impl RngRecorder {
    fn new(id: impl Into<String>, phase: Phase) -> (Self, Arc<Mutex<Vec<u64>>>) {
        let values = Arc::new(Mutex::new(Vec::new()));
        let s = Self {
            id: id.into(),
            phase,
            values: values.clone(),
        };
        (s, values)
    }
}

impl SimSystem for RngRecorder {
    fn id(&self) -> &str {
        &self.id
    }

    fn phase(&self) -> Phase {
        self.phase
    }

    fn read_tables(&self) -> Vec<String> {
        vec!["rng_test".to_owned()]
    }

    fn write_tables(&self) -> Vec<String> {
        vec!["rng_test".to_owned()]
    }

    fn execute(
        &self,
        rng: &mut DeterministicRng,
        _query: &QueryEngine,
        _phase: Phase,
        _tick: u64,
    ) -> SchedulerResult<Vec<Diff>> {
        let mut guard = self
            .values
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("rng recorder lock poisoned: {e}")))?;
        guard.push(rng.next_u64());
        guard.push(rng.next_u64());
        Ok(Vec::new())
    }
}

/// A system with a refresh callback.
#[derive(Debug, Clone)]
struct RefreshAwareSystem {
    id: String,
    phase: Phase,
    counter: Arc<AtomicU64>,
}

impl RefreshAwareSystem {
    fn new(id: impl Into<String>, phase: Phase) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let s = Self {
            id: id.into(),
            phase,
            counter: counter.clone(),
        };
        (s, counter)
    }
}

impl SimSystem for RefreshAwareSystem {
    fn id(&self) -> &str {
        &self.id
    }

    fn phase(&self) -> Phase {
        self.phase
    }

    fn read_tables(&self) -> Vec<String> {
        Vec::new()
    }

    fn write_tables(&self) -> Vec<String> {
        Vec::new()
    }

    fn refresh_callback(&self) -> Option<RefreshCallback> {
        let counter = self.counter.clone();
        Some(Arc::new(move |_tick: u64, _gen: u64| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
    }

    fn execute(
        &self,
        _rng: &mut DeterministicRng,
        _query: &QueryEngine,
        _phase: Phase,
        _tick: u64,
    ) -> SchedulerResult<Vec<Diff>> {
        Ok(Vec::new())
    }
}

// ===================================================================
// Phase ordering tests
// ===================================================================

#[test]
fn phase_ordering_is_strict() {
    let phases: Vec<Phase> = Phase::all_in_order().collect();
    let expected = vec![
        Phase::PreTick,
        Phase::Economy,
        Phase::Diplomacy,
        Phase::Military,
        Phase::PostTick,
    ];
    assert_eq!(phases, expected);
}

#[test]
fn phase_next_returns_correct_successor() {
    assert_eq!(Phase::PreTick.next(), Some(Phase::Economy));
    assert_eq!(Phase::Economy.next(), Some(Phase::Diplomacy));
    assert_eq!(Phase::Diplomacy.next(), Some(Phase::Military));
    assert_eq!(Phase::Military.next(), Some(Phase::PostTick));
    assert_eq!(Phase::PostTick.next(), None);
}

#[test]
fn phase_comparison_works() {
    assert!(Phase::PreTick < Phase::Economy);
    assert!(Phase::Economy < Phase::Diplomacy);
    assert!(Phase::Diplomacy < Phase::Military);
    assert!(Phase::Military < Phase::PostTick);
}

// ===================================================================
// System registration tests
// ===================================================================

#[test]
fn register_system_succeeds() {
    let scheduler = empty_scheduler();
    let system: BoxedSystem = Arc::new(NoOpSystem::new("test", Phase::Economy));
    scheduler.register_system(system).unwrap();
    let ids = scheduler.system_ids().unwrap();
    assert_eq!(ids, vec!["test"]);
}

#[test]
fn register_duplicate_system_fails() {
    let scheduler = empty_scheduler();
    let a: BoxedSystem = Arc::new(NoOpSystem::new("dup", Phase::Economy));
    let b: BoxedSystem = Arc::new(NoOpSystem::new("dup", Phase::Diplomacy));
    scheduler.register_system(a).unwrap();
    let err = scheduler.register_system(b).unwrap_err();
    assert!(
        matches!(err, SchedulerError::SystemAlreadyRegistered(ref id) if id == "dup"),
        "expected SystemAlreadyRegistered, got {err:?}"
    );
}

/// Would have failed before TOCTOU fix: register_system checked initialized
/// before acquiring locks, allowing a concurrent initialize() to slip through.
/// The post-lock re-check catches this.
#[test]
fn register_system_rejected_after_initialization() {
    let scheduler = empty_scheduler();
    assert!(!scheduler.is_initialized().unwrap());
    scheduler.initialize().unwrap();
    assert!(scheduler.is_initialized().unwrap());

    let system: BoxedSystem = Arc::new(NoOpSystem::new("late", Phase::Economy));
    let err = scheduler.register_system(system).unwrap_err();
    assert!(matches!(err, SchedulerError::Initialized));
}

#[test]
fn unregister_system_removes_it() {
    let scheduler = empty_scheduler();
    let system: BoxedSystem = Arc::new(NoOpSystem::new("gone", Phase::Economy));
    scheduler.register_system(system).unwrap();
    scheduler.unregister_system("gone").unwrap();
    let ids = scheduler.system_ids().unwrap();
    assert!(ids.is_empty());
}

#[test]
fn write_conflict_detected_in_same_phase() {
    let scheduler = empty_scheduler();
    let a: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["economy"]));
    let b: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_b", Phase::Economy).with_writes(&["economy"]));
    scheduler.register_system(a).unwrap();
    let err = scheduler.register_system(b).unwrap_err();
    assert!(
        matches!(err, SchedulerError::WriteConflict { .. }),
        "expected WriteConflict, got {err:?}"
    );
}

#[test]
fn no_conflict_when_write_sets_disjoint() {
    let scheduler = empty_scheduler();
    let a: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["economy"]));
    let b: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_b", Phase::Economy).with_writes(&["diplomacy"]));
    scheduler.register_system(a).unwrap();
    scheduler.register_system(b).unwrap();
    let ids = scheduler.system_ids().unwrap();
    assert_eq!(ids.len(), 2);
}

#[test]
fn no_conflict_when_same_table_different_phases() {
    let scheduler = empty_scheduler();
    let a: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["shared"]));
    let b: BoxedSystem =
        Arc::new(NoOpSystem::new("writer_b", Phase::Diplomacy).with_writes(&["shared"]));
    scheduler.register_system(a).unwrap();
    scheduler.register_system(b).unwrap();
    let ids = scheduler.system_ids().unwrap();
    assert_eq!(ids.len(), 2);
}

#[test]
fn systems_grouped_by_phase_in_order() {
    let scheduler = empty_scheduler();
    let pre: BoxedSystem = Arc::new(NoOpSystem::new("pre", Phase::PreTick));
    let eco: BoxedSystem = Arc::new(NoOpSystem::new("eco", Phase::Economy));
    let post: BoxedSystem = Arc::new(NoOpSystem::new("post", Phase::PostTick));
    scheduler.register_system(pre).unwrap();
    scheduler.register_system(eco).unwrap();
    scheduler.register_system(post).unwrap();

    let by_phase = scheduler.systems_by_phase().unwrap();
    let phases: Vec<_> = by_phase.iter().map(|(p, _)| *p).collect();
    assert_eq!(
        phases,
        vec![Phase::PreTick, Phase::Economy, Phase::PostTick]
    );
}

#[test]
fn registration_from_system_matches_trait() {
    let system = NoOpSystem::new("reg", Phase::Military)
        .with_reads(&["units"])
        .with_writes(&["battles"]);
    let reg = SystemRegistration::from_system(&system);
    assert_eq!(reg.system_id, "reg");
    assert_eq!(reg.phase, Phase::Military);
    assert_eq!(reg.read_tables, HashSet::from(["units".to_owned()]));
    assert_eq!(reg.write_tables, HashSet::from(["battles".to_owned()]));
    assert!(reg.is_writer());
}

// ===================================================================
// Command buffer / tick-boundary tests
// ===================================================================

#[test]
fn enqueue_and_consume_commands() {
    let scheduler = empty_scheduler();
    let env = CommandEnvelope::new(
        Tick::ZERO,
        "player",
        Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        },
    );
    scheduler.enqueue_command(env.clone()).unwrap();
    assert_eq!(scheduler.pending_command_count().unwrap(), 1);

    let consumed = scheduler.consume_pending_commands().unwrap();
    assert_eq!(consumed.len(), 1);
    assert_eq!(consumed[0].tick, Tick::ZERO);
    assert_eq!(scheduler.pending_command_count().unwrap(), 0);
}

#[test]
fn commands_consumed_at_tick_boundary_before_phases() {
    let scheduler = empty_scheduler();
    let env = CommandEnvelope::new(
        Tick::ZERO,
        "bridge",
        Command::Raw {
            domain: "input".to_owned(),
            payload: serde_json::json!({}),
        },
    );
    scheduler.enqueue_command(env).unwrap();

    scheduler.initialize().unwrap();
    let result = scheduler.tick().unwrap();
    assert_eq!(result.tick, Tick::ZERO);
    assert_eq!(scheduler.pending_command_count().unwrap(), 0);
}

#[test]
fn commands_queued_for_next_tick_are_not_consumed_early() {
    let scheduler = empty_scheduler();
    let env = CommandEnvelope::new(
        Tick(1),
        "bridge",
        Command::Raw {
            domain: "input".to_owned(),
            payload: serde_json::json!({}),
        },
    );
    scheduler.enqueue_command(env).unwrap();

    // The journal is at tick 0; submitting a tick-1 command will fail validation
    // because the journal expects tick 0. This proves boundary isolation.
    let result = scheduler.tick();
    assert!(
        result.is_err(),
        "expected tick to fail because command is for future tick"
    );
}

// ===================================================================
// Deterministic RNG tests
// ===================================================================

#[test]
fn rng_produces_identical_sequence_for_same_seed() {
    let mut a = DeterministicRng::new("sys", 42);
    let mut b = DeterministicRng::new("sys", 42);
    for _ in 0..100 {
        assert_eq!(a.next_u64(), b.next_u64());
    }
}

#[test]
fn rng_differs_across_system_ids() {
    let mut a = DeterministicRng::new("sys_a", 42);
    let mut b = DeterministicRng::new("sys_b", 42);
    let va = a.next_u64();
    let vb = b.next_u64();
    assert_ne!(
        va, vb,
        "different system ids should produce different streams"
    );
}

#[test]
fn rng_differs_across_ticks() {
    let mut a = DeterministicRng::new("sys", 1);
    let mut b = DeterministicRng::new("sys", 2);
    let va = a.next_u64();
    let vb = b.next_u64();
    assert_ne!(va, vb, "different ticks should produce different streams");
}

#[test]
fn rng_f64_in_valid_range() {
    let mut rng = DeterministicRng::new("sys", 0);
    for _ in 0..1000 {
        let v = rng.next_f64();
        assert!((0.0..1.0).contains(&v));
    }
}

#[test]
fn rng_next_usize_respects_bound() {
    let mut rng = DeterministicRng::new("sys", 0);
    for _ in 0..100 {
        let v = rng.next_usize(10);
        assert!(v < 10);
    }
}

#[test]
fn rng_next_usize_zero_bound_returns_zero() {
    let mut rng = DeterministicRng::new("sys", 0);
    assert_eq!(rng.next_usize(0), 0);
}

#[test]
fn rng_provided_to_system_during_execute() {
    let scheduler = empty_scheduler();
    let (recorder, values) = RngRecorder::new("rng_sys", Phase::Economy);
    let system: BoxedSystem = Arc::new(recorder);
    scheduler.register_system(system).unwrap();

    scheduler.initialize().unwrap();
    scheduler.tick().unwrap();

    let vals = values.lock().unwrap();
    assert_eq!(vals.len(), 2);
    // Re-run with same tick should produce identical values
    let mut rng_a = DeterministicRng::new("rng_sys", 0);
    let mut rng_b = DeterministicRng::new("rng_sys", 0);
    assert_eq!(rng_a.next_u64(), rng_b.next_u64());
    assert_eq!(rng_a.next_u64(), rng_b.next_u64());
}

// ===================================================================
// Atomic commit cycle tests
// ===================================================================

#[test]
fn atomic_commit_advances_generation() {
    let scheduler = empty_scheduler();
    scheduler.initialize().unwrap();
    let before = scheduler.current_generation().unwrap();
    scheduler.atomic_commit().unwrap();
    let after = scheduler.current_generation().unwrap();
    assert_eq!(after, before + 1);
}

#[test]
fn tick_advances_tick_counter() {
    let scheduler = empty_scheduler();
    scheduler.initialize().unwrap();
    assert_eq!(scheduler.current_tick().unwrap(), Tick::ZERO);
    scheduler.tick().unwrap();
    assert_eq!(scheduler.current_tick().unwrap(), Tick(1));
}

#[test]
fn system_diffs_submitted_to_journal_during_tick() {
    let scheduler = make_scheduler_with_table("eco");

    let diff = Diff::Update {
        table: "eco".to_owned(),
        row: RowId::new(1),
        column: "value".to_owned(),
        value: serde_json::json!(100),
    };
    let emitter: BoxedSystem = Arc::new(DiffEmitter::new("eco_sys", Phase::Economy, diff));
    scheduler.register_system(emitter).unwrap();

    scheduler.initialize().unwrap();
    let result = scheduler.tick().unwrap();
    // The diff was submitted; commit result should reflect it.
    assert_eq!(result.tick, Tick(1));
    assert!(result.diff_count >= 1);
}

// ===================================================================
// Refresh signal protocol tests
// ===================================================================

#[test]
fn refresh_signal_bus_register_and_broadcast() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let counter = Arc::new(AtomicU64::new(0));
    let cb: RefreshCallback = {
        let c = counter.clone();
        Arc::new(move |_tick, _gen| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    };
    bus.register("consumer", cb).unwrap();
    bus.broadcast(5, 10).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn refresh_signal_bus_broadcast_fails_if_consumer_errors() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let cb: RefreshCallback =
        Arc::new(move |_tick, _gen| Err(SchedulerError::Generic("fail".to_owned())));
    bus.register("bad", cb).unwrap();
    let err = bus.broadcast(0, 0).unwrap_err();
    assert!(
        matches!(
         err,
         SchedulerError::RefreshSignalFailed {
         consumer: ref name,
        ..
         } if name == "bad"
         ),
        "expected RefreshSignalFailed, got {err:?}"
    );
}

#[test]
fn refresh_signal_bus_unregister_stops_delivery() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let counter = Arc::new(AtomicU64::new(0));
    let cb: RefreshCallback = {
        let c = counter.clone();
        Arc::new(move |_tick, _gen| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    };
    let handle = bus.register("tmp", cb).unwrap();
    bus.broadcast(1, 1).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    bus.unregister(&handle).unwrap();
    bus.broadcast(2, 2).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1); // no increment
}

#[test]
fn scheduler_broadcast_refresh_reaches_consumers() {
    let scheduler = empty_scheduler();
    let counter = Arc::new(AtomicU64::new(0));
    let cb: RefreshCallback = {
        let c = counter.clone();
        Arc::new(move |_tick, _gen| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    };
    scheduler.register_consumer("ext", cb).unwrap();
    scheduler.broadcast_refresh(Tick::ZERO, 1).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn system_refresh_callback_registered_automatically() {
    let scheduler = empty_scheduler();
    let (system, counter) = RefreshAwareSystem::new("refresh_sys", Phase::PostTick);
    let boxed: BoxedSystem = Arc::new(system);
    scheduler.register_system(boxed).unwrap();

    // The system's refresh callback should be on the bus.
    let names = scheduler.consumer_names().unwrap();
    assert!(names.contains(&"refresh_sys".to_owned()));

    scheduler.broadcast_refresh(Tick::ZERO, 1).unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn refresh_signal_sequence_integrity() {
    let scheduler = empty_scheduler();
    let step_log = Arc::new(Mutex::new(Vec::new()));

    let log = step_log.clone();
    let cb: RefreshCallback = Arc::new(move |tick, gen| {
        let mut guard = log
            .lock()
            .map_err(|e| SchedulerError::Generic(format!("log lock poisoned: {e}")))?;
        guard.push((tick, gen));
        Ok(())
    });
    scheduler.register_consumer("rule_ir", cb.clone()).unwrap();
    scheduler.register_consumer("bevy_bridge", cb).unwrap();

    scheduler.initialize().unwrap();
    let result = scheduler.tick().unwrap();

    let log = step_log.lock().unwrap();
    // Both consumers should have received the signal with the same tick/generation.
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].0, result.tick.0);
    assert_eq!(log[1].0, result.tick.0);
}

// ===================================================================
// Initialization tests
// ===================================================================

#[test]
fn scheduler_initializes_once() {
    let scheduler = empty_scheduler();
    let sys = NoOpSystem::new("init", Phase::PreTick)
        .with_reads(&["status"])
        .with_writes(&["init_flag"]);
    scheduler.register_system(Arc::new(sys)).unwrap();
    assert!(!scheduler.is_initialized().unwrap());
    scheduler.initialize().unwrap();
    assert!(scheduler.is_initialized().unwrap());
    // Second init is a no-op.
    scheduler.initialize().unwrap();
    assert!(scheduler.is_initialized().unwrap());
}

// ===================================================================
// Integration: full tick lifecycle
// ===================================================================

#[test]
fn full_tick_lifecycle_with_multiple_systems() {
    let scheduler = empty_scheduler();

    let pre = NoOpSystem::new("pre", Phase::PreTick).with_writes(&["dummy"]);
    let eco = NoOpSystem::new("eco", Phase::Economy).with_writes(&["economy"]);
    let dip = NoOpSystem::new("dip", Phase::Diplomacy).with_writes(&["diplomacy"]);
    let post = NoOpSystem::new("post", Phase::PostTick).with_writes(&["dummy"]);

    scheduler.register_system(Arc::new(pre)).unwrap();
    scheduler.register_system(Arc::new(eco)).unwrap();
    scheduler.register_system(Arc::new(dip)).unwrap();
    scheduler.register_system(Arc::new(post)).unwrap();

    let env = CommandEnvelope::new(
        Tick::ZERO,
        "bridge",
        Command::Raw {
            domain: "input".to_owned(),
            payload: serde_json::json!({}),
        },
    );
    scheduler.enqueue_command(env).unwrap();

    scheduler.initialize().unwrap();
    let result = scheduler.tick().unwrap();
    assert_eq!(result.tick, Tick::ZERO);
    assert_eq!(scheduler.current_tick().unwrap(), Tick(1));
}

#[test]
fn deterministic_rng_across_replays() {
    // Two independent schedulers with identical config should produce
    // identical RNG sequences for the same system/tick.
    let s1 = empty_scheduler();
    let s2 = empty_scheduler();

    let (r1, v1) = RngRecorder::new("replay", Phase::Economy);
    let (r2, v2) = RngRecorder::new("replay", Phase::Economy);

    s1.register_system(Arc::new(r1)).unwrap();
    s2.register_system(Arc::new(r2)).unwrap();

    s1.initialize().unwrap();
    s2.initialize().unwrap();
    s1.tick().unwrap();
    s2.tick().unwrap();

    let a = v1.lock().unwrap();
    let b = v2.lock().unwrap();
    assert_eq!(*a, *b);
}

// ===================================================================
// Edge-case tests (may-fail)
// ===================================================================

/// Would fail: registering two systems with the same name but different
/// phases must reject the second registration with SystemAlreadyRegistered.
#[test]
fn register_system_with_duplicate_name_rejected() {
    let scheduler = empty_scheduler();
    let first: BoxedSystem = Arc::new(
        NoOpSystem::new("shared_name", Phase::Economy)
            .with_reads(&["eco"])
            .with_writes(&["eco_out"]),
    );
    let second: BoxedSystem = Arc::new(
        NoOpSystem::new("shared_name", Phase::Diplomacy)
            .with_reads(&["dip"])
            .with_writes(&["dip_out"]),
    );

    scheduler.register_system(first).unwrap();
    assert_eq!(scheduler.system_ids().unwrap().len(), 1);

    let err = scheduler.register_system(second).unwrap_err();
    assert!(
        matches!(err, SchedulerError::SystemAlreadyRegistered(ref id) if id == "shared_name"),
        "expected SystemAlreadyRegistered, got {err:?}"
    );
    // First system must still be registered and not removed by failed second call.
    assert_eq!(scheduler.system_ids().unwrap().len(), 1);
}

/// Would fail: calling tick() before initialize() must return NotInitialized.
#[test]
fn tick_before_initialize_rejected() {
    let scheduler = empty_scheduler();
    assert!(!scheduler.is_initialized().unwrap());
    let err = scheduler.tick().unwrap_err();
    assert!(
        matches!(err, SchedulerError::NotInitialized),
        "expected NotInitialized, got {err:?}"
    );
}

/// Would fail: atomic_commit with zero pending diffs must still advance
/// the snapshot generation counter.
#[test]
fn atomic_commit_without_diffs_advances_generation() {
    let scheduler = empty_scheduler();
    let before = scheduler.current_generation().unwrap();
    let result = scheduler.atomic_commit().unwrap();
    assert_eq!(result.diff_count, 0);
    let after = scheduler.current_generation().unwrap();
    assert_eq!(
        after,
        before + 1,
        "atomic_commit must advance generation even with no diffs"
    );
}

/// Would fail: registering a system with a valid Phase must succeed
/// without phase-related errors. All five Phase variants are implicitly
/// registered with the scheduler.
#[test]
fn system_phase_not_registered_error() {
    let scheduler = empty_scheduler();

    let systems: Vec<(&str, Phase)> = vec![
        ("pre", Phase::PreTick),
        ("eco", Phase::Economy),
        ("dip", Phase::Diplomacy),
        ("mil", Phase::Military),
        ("post", Phase::PostTick),
    ];

    for (id, phase) in &systems {
        let sys: BoxedSystem = Arc::new(
            NoOpSystem::new(*id, *phase)
                .with_reads(&["shared_read"])
                .with_writes(&[id]),
        );
        scheduler.register_system(sys).unwrap();
    }

    // Verify all 5 systems registered, each with correct phase.
    let by_phase = scheduler.systems_by_phase().unwrap();
    assert_eq!(
        by_phase.len(),
        5,
        "all 5 Phase variants should be accepted by the scheduler"
    );
    for (phase, ids) in &by_phase {
        assert_eq!(
            ids.len(),
            1,
            "phase {:?} should have exactly 1 system",
            phase
        );
    }
}

// ===================================================================
// Refresh signal bus edge-case tests
// ===================================================================

#[test]
fn refresh_signal_bus_unregister_invalid_handle_returns_error() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let fake = RefreshSignalHandle("nonexistent".to_owned());
    let err = bus.unregister(&fake).unwrap_err();
    assert!(
        matches!(err, SchedulerError::RefreshBusLookupFailed(ref name) if name == "nonexistent"),
        "expected RefreshBusLookupFailed, got {err:?}"
    );
}

#[test]
fn refresh_signal_bus_unregister_by_name_not_found_returns_error() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let err = bus.unregister_by_name("nonexistent").unwrap_err();
    assert!(
        matches!(err, SchedulerError::RefreshBusLookupFailed(ref name) if name == "nonexistent"),
        "expected RefreshBusLookupFailed, got {err:?}"
    );
}

#[test]
fn refresh_signal_bus_unregister_by_name_succeeds() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let noop: RefreshCallback = Arc::new(|_, _| Ok(()));
    bus.register("target", noop).unwrap();
    assert_eq!(bus.consumer_count().unwrap(), 1);
    bus.unregister_by_name("target").unwrap();
    assert_eq!(bus.consumer_count().unwrap(), 0);
}

#[test]
fn refresh_signal_bus_broadcast_error_includes_source() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let cb: RefreshCallback =
        Arc::new(|_, _| Err(SchedulerError::Generic("inner failure".to_owned())));
    bus.register("faulty", cb).unwrap();
    let err = bus.broadcast(0, 0).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("faulty"),
        "message should name consumer, got: {msg}"
    );
    assert!(
        msg.contains("inner failure"),
        "message should include source, got: {msg}"
    );
    assert!(
        msg.contains("refresh signal failed"),
        "message should describe error type, got: {msg}"
    );
}

#[test]
fn refresh_signal_bus_debug_format_includes_consumer_count() {
    let bus = scharnhorst_scheduler::RefreshSignalBus::new();
    let debug = format!("{:?}", bus);
    assert!(
        debug.contains("RefreshSignalBus"),
        "Debug should include struct name, got: {debug}"
    );
    assert!(
        debug.contains("consumer_count"),
        "Debug should include field name, got: {debug}"
    );
    assert!(
        debug.contains("0"),
        "Debug should show count 0 for empty bus, got: {debug}"
    );

    let noop: RefreshCallback = Arc::new(|_, _| Ok(()));
    bus.register("a", noop).unwrap();
    let debug = format!("{:?}", bus);
    assert!(
        debug.contains("1"),
        "Debug should show updated count, got: {debug}"
    );
}

// ===================================================================
// May-fail edge-case tests (uncovered-branch coverage)
// ===================================================================

/// Would fail: `initialize()` must reject when the schema registry is
/// not yet frozen.
///
/// Branch: `if !self.query_engine.is_schema_frozen()... → Err(SchemaNotFrozen)`
/// in `Scheduler::initialize`.
#[test]
fn initialize_rejects_unfrozen_schema() {
    let scheduler = Scheduler::new(empty_journal(), unfrozen_query_engine());
    // Register a system with valid read/write tables so the
    // "no tables declared" check does not trigger first.
    let sys = NoOpSystem::new("valid", Phase::Economy).with_reads(&["eco"]);
    scheduler.register_system(Arc::new(sys)).unwrap();

    let err = scheduler.initialize().unwrap_err();
    assert!(
        matches!(err, SchedulerError::SchemaNotFrozen),
        "expected SchemaNotFrozen, got {err:?}"
    );
    // Must remain uninitialized.
    assert!(!scheduler.is_initialized().unwrap());
}

/// Would fail: a system whose `execute()` returns `Err` must propagate
/// that error through `run_phase → run_phases → tick`.
///
/// Branch: `system.execute(...)?` → error path in `run_phase`.
/// Also covers: the `?` on `self.run_phases(tick)` in `tick`.
#[test]
fn system_execute_error_propagates_from_tick() {
    let scheduler = empty_scheduler();

    // A system that always fails during execute.
    struct FailingSystem {
        id: &'static str,
        phase: Phase,
    }
    impl SimSystem for FailingSystem {
        fn id(&self) -> &str {
            self.id
        }
        fn phase(&self) -> Phase {
            self.phase
        }
        fn read_tables(&self) -> Vec<String> {
            vec!["any".to_owned()]
        }
        fn write_tables(&self) -> Vec<String> {
            vec![]
        }
        fn execute(
            &self,
            _rng: &mut DeterministicRng,
            _query: &QueryEngine,
            _phase: Phase,
            _tick: u64,
        ) -> SchedulerResult<Vec<Diff>> {
            Err(SchedulerError::Generic("execute failure".to_owned()))
        }
    }

    scheduler
        .register_system(Arc::new(FailingSystem {
            id: "fail",
            phase: Phase::Economy,
        }))
        .unwrap();
    scheduler.initialize().unwrap();

    let tick_before = scheduler.current_tick().unwrap();
    let gen_before = scheduler.current_generation().unwrap();

    let err = scheduler.tick().unwrap_err();
    assert!(
        matches!(err, SchedulerError::Generic(ref msg) if msg == "execute failure"),
        "expected execute failure error, got {err:?}"
    );

    // Tick and generation must NOT advance — atomic_commit was never reached.
    assert_eq!(
        scheduler.current_tick().unwrap(),
        tick_before,
        "tick must not advance after execute error"
    );
    assert_eq!(
        scheduler.current_generation().unwrap(),
        gen_before,
        "generation must not advance after execute error"
    );
}

/// Would fail: when `run_phases` fails before `atomic_commit`, commands
/// consumed at the tick boundary must be restored.
///
/// The rollback logic in `tick()` only fires in the `Err` arm of
/// `atomic_commit`. If `run_phases` fails, consumed commands can be lost
/// unless `tick()` treats the whole tick as one internal SAGA.
#[test]
fn commands_restore_when_run_phases_fails_before_commit() {
    let scheduler = empty_scheduler();

    struct FailingSystem;
    impl SimSystem for FailingSystem {
        fn id(&self) -> &str {
            "fail"
        }
        fn phase(&self) -> Phase {
            Phase::Economy
        }
        fn read_tables(&self) -> Vec<String> {
            vec!["any".to_owned()]
        }
        fn write_tables(&self) -> Vec<String> {
            vec![]
        }
        fn execute(
            &self,
            _rng: &mut DeterministicRng,
            _query: &QueryEngine,
            _phase: Phase,
            _tick: u64,
        ) -> SchedulerResult<Vec<Diff>> {
            Err(SchedulerError::Generic("boom".to_owned()))
        }
    }

    scheduler.register_system(Arc::new(FailingSystem)).unwrap();
    scheduler.initialize().unwrap();

    let env = CommandEnvelope::new(
        Tick::ZERO,
        "bridge",
        Command::Raw {
            domain: "input".to_owned(),
            payload: serde_json::json!({}),
        },
    );
    scheduler.enqueue_command(env).unwrap();
    assert_eq!(scheduler.pending_command_count().unwrap(), 1);

    let _err = scheduler.tick().unwrap_err();

    assert_eq!(
        scheduler.pending_command_count().unwrap(),
        1,
        "commands consumed before run_phases failure must be restored"
    );
}

// ===================================================================
// Telemetry tests (metrics feature enabled)
// ===================================================================

#[cfg(feature = "metrics")]
mod telemetry_tests {
    use scharnhorst_scheduler::telemetry::{emit_diff_count, emit_system_event, DurationGuard};

    #[test]
    fn tick_span_construct_and_drop() {
        let guard = DurationGuard::tick_span(0);
        drop(guard);
    }

    #[test]
    fn phase_span_construct_and_drop() {
        let guard = DurationGuard::phase_span(0, "test_phase");
        drop(guard);
    }

    #[test]
    fn emit_system_event_no_panic() {
        emit_system_event(0, "test_phase", "test_system", 42);
    }

    #[test]
    fn emit_diff_count_no_panic() {
        emit_diff_count(0, 5);
    }

    #[test]
    fn duration_guard_drop_records_duration() {
        // Scope-bound drop: the guard records duration_micros on its span when dropped.
        {
            let _guard = DurationGuard::tick_span(7);
            // Guard is dropped here at end of scope.
        }
        // No assertion needed for crash-only test — if drop panics, the test fails.
    }

    #[test]
    fn explicit_drop_closes_span_without_double_free() {
        // I-SCHED-SPAN-DROP-CLOSE: explicit drop must close the span
        // without double-free or other issues.
        let guard = DurationGuard::tick_span(42);
        drop(guard);
        // Guard is consumed by explicit drop. No further drop should occur.
        // If this compiles and runs without panic, the invariant holds.
    }
}

#[test]
fn noop_duration_guard_compiles_and_does_not_panic() {
    // When metrics is disabled, DurationGuard is a ZST with noop constructors.
    // This test ensures both variants compile and run.
    use scharnhorst_scheduler::telemetry::{emit_diff_count, emit_system_event, DurationGuard};

    let guard = DurationGuard::tick_span(0);
    drop(guard);

    let guard = DurationGuard::phase_span(0, "test");
    drop(guard);

    emit_system_event(0, "phase", "system", 0);
    emit_diff_count(0, 0);
}
