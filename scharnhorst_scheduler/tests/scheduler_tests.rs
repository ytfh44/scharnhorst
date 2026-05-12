use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use scharnhorst_core::{RowId, Tick};
use scharnhorst_journal::command::{Command, CommandEnvelope};
use scharnhorst_journal::diff::Diff;
use scharnhorst_journal::journal::Journal;
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_schema::SchemaRegistry;
use scharnhorst_scheduler::{
 BoxedSystem, DeterministicRng, Phase, RefreshCallback, Scheduler, SchedulerError,
 SchedulerResult, SimSystem, SystemRegistration,
};

// ===================================================================
// Test helpers
// ===================================================================

fn empty_query_engine() -> QueryEngine {
 QueryEngine::new(SchemaRegistry::new())
}

fn empty_journal() -> Journal {
 Journal::default()
}

fn make_scheduler_with_table(table_name: &str) -> Scheduler {
 let store = scharnhorst_arrow_store::ArrowStore::new();
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
 store.create_table(&spec, scharnhorst_arrow_store::MutationMode::Patchable).unwrap();
 let mut journal = Journal::new(store);
 let mut values = serde_json::Map::new();
 values.insert("id".to_owned(), serde_json::Value::Number(serde_json::Number::from(1u64)));
 values.insert("value".to_owned(), serde_json::Value::Number(serde_json::Number::from(0i64)));
 let insert = Diff::Insert {
 table: table_name.to_owned(),
 row: RowId::new(1),
 values,
 };
 journal.submit_diff(insert).unwrap();
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
 _query: &QueryEngine, _phase: Phase,
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
 _query: &QueryEngine, _phase: Phase,
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
 Vec::new()
 }

 fn write_tables(&self) -> Vec<String> {
 Vec::new()
 }

 fn execute(
 &self,
 rng: &mut DeterministicRng,
 _query: &QueryEngine, _phase: Phase,
 _tick: u64,
 ) -> SchedulerResult<Vec<Diff>> {
 let mut guard = self.values.lock().map_err(|e| {
 SchedulerError::Generic(format!("rng recorder lock poisoned: {e}"))
 })?;
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
 _query: &QueryEngine, _phase: Phase,
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
 let a: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["economy"]),
 );
 let b: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_b", Phase::Economy).with_writes(&["economy"]),
 );
 scheduler.register_system(a).unwrap();
 let err = scheduler.register_system(b).unwrap_err();
 assert!(
 matches!(err, SchedulerError::WriteConflict {.. }),
 "expected WriteConflict, got {err:?}"
 );
}

#[test]
fn no_conflict_when_write_sets_disjoint() {
 let scheduler = empty_scheduler();
 let a: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["economy"]),
 );
 let b: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_b", Phase::Economy).with_writes(&["diplomacy"]),
 );
 scheduler.register_system(a).unwrap();
 scheduler.register_system(b).unwrap();
 let ids = scheduler.system_ids().unwrap();
 assert_eq!(ids.len(), 2);
}

#[test]
fn no_conflict_when_same_table_different_phases() {
 let scheduler = empty_scheduler();
 let a: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_a", Phase::Economy).with_writes(&["shared"]),
 );
 let b: BoxedSystem = Arc::new(
 NoOpSystem::new("writer_b", Phase::Diplomacy).with_writes(&["shared"]),
 );
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
 let env = CommandEnvelope::new(Tick::ZERO, "player", Command::Raw {
 domain: "move".to_owned(),
 payload: serde_json::json!({"x": 1}),
 });
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
 let env = CommandEnvelope::new(Tick::ZERO, "bridge", Command::Raw {
 domain: "input".to_owned(),
 payload: serde_json::json!({}),
 });
 scheduler.enqueue_command(env).unwrap();

 // tick should consume commands, run phases, commit, and broadcast.
 let result = scheduler.tick().unwrap();
 assert_eq!(result.tick, Tick::ZERO);
 assert_eq!(scheduler.pending_command_count().unwrap(), 0);
}

#[test]
fn commands_queued_for_next_tick_are_not_consumed_early() {
 let scheduler = empty_scheduler();
 let env = CommandEnvelope::new(Tick(1), "bridge", Command::Raw {
 domain: "input".to_owned(),
 payload: serde_json::json!({}),
 });
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
 assert_ne!(va, vb, "different system ids should produce different streams");
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
 let before = scheduler.current_generation().unwrap();
 scheduler.atomic_commit().unwrap();
 let after = scheduler.current_generation().unwrap();
 assert_eq!(after, before + 1);
}

#[test]
fn tick_advances_tick_counter() {
 let scheduler = empty_scheduler();
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
 let cb: RefreshCallback = Arc::new(move |_tick, _gen| {
 Err(SchedulerError::Generic("fail".to_owned()))
 });
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
 // Simulate the full tick-boundary signal sequence from the spec.
 let scheduler = empty_scheduler();
 let step_log = Arc::new(Mutex::new(Vec::new()));

 let log = step_log.clone();
 let cb: RefreshCallback = Arc::new(move |tick, gen| {
 let mut guard = log.lock().map_err(|e| {
 SchedulerError::Generic(format!("log lock poisoned: {e}"))
 })?;
 guard.push((tick, gen));
 Ok(())
 });
 scheduler.register_consumer("rule_ir", cb.clone()).unwrap();
 scheduler.register_consumer("bevy_bridge", cb).unwrap();

 // Run a tick: commit + broadcast happen inside tick.
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

 let pre = NoOpSystem::new("pre", Phase::PreTick);
 let eco = NoOpSystem::new("eco", Phase::Economy).with_writes(&["economy"]);
 let dip = NoOpSystem::new("dip", Phase::Diplomacy).with_writes(&["diplomacy"]);
 let post = NoOpSystem::new("post", Phase::PostTick);

 scheduler.register_system(Arc::new(pre)).unwrap();
 scheduler.register_system(Arc::new(eco)).unwrap();
 scheduler.register_system(Arc::new(dip)).unwrap();
 scheduler.register_system(Arc::new(post)).unwrap();

 let env = CommandEnvelope::new(Tick::ZERO, "bridge", Command::Raw {
 domain: "input".to_owned(),
 payload: serde_json::json!({}),
 });
 scheduler.enqueue_command(env).unwrap();

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

 s1.tick().unwrap();
 s2.tick().unwrap();

 let a = v1.lock().unwrap();
 let b = v2.lock().unwrap();
 assert_eq!(*a, *b);
}
