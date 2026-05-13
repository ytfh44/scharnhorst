# Sim Scheduler — Lock-Free Optimization Delta

## Overview

This spec describes the behavioral implications of Phase 1 (atomic replacements for `current_tick`, `generation`, `initialized`) and Phase 2 (RefreshSignalBus broadcast snapshotting). All three fields currently wrapped in `Arc<Mutex<T>>` are replaced with `Arc<AtomicU64>` / `Arc<AtomicBool>`.

**Baseline**: [openspec/specs/sim-scheduler/spec.md](openspec/specs/sim-scheduler/spec.md)

---

## S-001: Tick Lifecycle

### Baseline Requirement

The scheduler maintains a `current_tick` that advances monotonically. `tick()` returns the tick before advance, `advance_tick()` increments by 1 and returns the new value.

### Behavioral Impact

**Lock acquisition eliminated**: The old `current_tick()` at [scheduler.rs:L273-L276](scharnhorst_scheduler/src/scheduler.rs#L273-L276):
```rust
pub fn current_tick(&self) -> SchedulerResult<Tick> {
    let tick = self.lock_tick()?;
    Ok(*tick)
}
```
Now uses `AtomicU64::load(Relaxed)`:
```rust
pub fn current_tick(&self) -> SchedulerResult<Tick> {
    Ok(Tick(self.current_tick.load(Ordering::Relaxed)))
}
```

**Return value equivalence**: `AtomicU64::load(Relaxed)` returns the same value as `*MutexGuard<Tick>` — both observe the latest stored value. Since ticks are only modified by `advance_tick()` and `advance_tick()` is only called from `tick()` at [scheduler.rs:L216](scharnhorst_scheduler/src/scheduler.rs#L216), there is no concurrent modification to observe stale values from.

**Evidence**: The `tick()` lifecycle at [scheduler.rs:L210-L218](scharnhorst_scheduler/src/scheduler.rs#L210-L218) calls `self.current_tick()` first (line 211), then at the very end calls `self.advance_tick()` (line 216). No code path modifies `current_tick` except `advance_tick()`. All consumers of `current_tick()` (e.g., `initialize()` at line 361 uses `_phase_order = reg.phase.order_index()`) see the tick at which their phase started.

**Advance semantics**: Old `advance_tick()` at [scheduler.rs:L266-L270](scharnhorst_scheduler/src/scheduler.rs#L266-L270):
```rust
let mut tick = self.lock_tick()?;
*tick = tick.next();
Ok(*tick)
```
New: `fetch_add(1, Relaxed)` returns the *old* value; the new tick is computed as `Tick(old).next()`:
```rust
let next = self.current_tick.fetch_add(1, Ordering::Relaxed);
let new_tick = Tick(next).next();
Ok(new_tick)
```
Both produce the same result (the new tick value). The use of `.next()` — which delegates to `Tick::saturating_add(1)` — rather than a raw `old + 1` computation ensures the return value follows `Tick`'s saturation semantics at the type level (physically unreachable at u64 scale).

**Monotonicity**: The old code achieved monotonicity through the Mutex serializing all writers. The new code achieves it through `AtomicU64::fetch_add` which guarantees atomic read-modify-write. Both are strictly monotonic; `fetch_add` additionally guarantees no lost updates under concurrent access (not that concurrent access occurs in practice).

---

## S-002: Atomic Commit

### Baseline Requirement

`atomic_commit()` commissions the journal, producing a `CommitResult`, then increments the snapshot generation counter.

### Behavioral Impact

**Generation counter**: Old code at [scheduler.rs:L252-L258](scharnhorst_scheduler/src/scheduler.rs#L252-L258):
```rust
let mut gen = self.lock_generation()?;
*gen = gen.saturating_add(1);
```
New: `self.generation.fetch_add(1, Ordering::Relaxed)`. The `saturating_add(1)` was defensive (u64 overflow is impossible in practice: 1M commits/sec × 584 billion years). `fetch_add` uses wrapping arithmetic (identical to `saturating_add` on u64 since overflow practically never occurs).

**Error path**: The old code could fail if the Mutex was poisoned. The new code cannot fail — `AtomicU64::fetch_add` never returns an error. This eliminates the `let mut gen = self.lock_generation()?;` error propagation path. The `current_generation()` method at [scheduler.rs:L279-L282](scharnhorst_scheduler/src/scheduler.rs#L279-L282) similarly becomes infallible (though the return type `SchedulerResult<u64>` is preserved for API compatibility — the `Ok()` wrapper is retained).

**Evidence**: `atomic_commit()` is called exclusively from `tick()` at [scheduler.rs:L214](scharnhorst_scheduler/src/scheduler.rs#L214), which is `pub fn tick(&self)`. Since `tick()` itself requires `&self`, callers share an `Arc<Scheduler>` and only one thread can call `tick()` at a time (Bevy systems run sequentially on the main thread for non-parallel resources). Generation counter atomicity is purely defensive.

---

## S-003: Initialization

### Baseline Requirement

`initialize()` validates all registered systems and their write sets, preventing writes on tables not declared. Idempotent: subsequent calls are no-ops.

### Behavioral Impact

**Idempotency mechanism change**: Old code at [scheduler.rs:L316-L366](scharnhorst_scheduler/src/scheduler.rs#L316-L366):
```rust
let mut init = self.lock_initialized()?;
if *init {
    return Ok(());
}
// ... validation ...
*init = true;
```
The Mutex serializes all callers. First caller does validation; all subsequent callers see `*init == true` and return early.

New code uses a two-phase `load`-then-`store` pattern (Amendment E / S-CA-001):
```rust
// Phase 1: fast-path check
if self.initialized.load(Ordering::Acquire) {
    return Ok(());
}
// Phase 2: validation under Mutex serialization
let systems = self.lock_systems()?;
let registrations = self.lock_registrations()?;
// ... validation ...
self.initialized.store(true, Ordering::Release);
Ok(())
```
**Why not compare_exchange**: A `compare_exchange(false, true, Acquire, Relaxed)` would permanently mask validation failures: if the CAS succeeded but validation subsequently failed, the `initialized` flag would remain `true` while no actual initialization completed. The two-phase pattern uses `Acquire`/`Release` ordering matching the original Mutex semantics but avoids this masking issue. The `load(Acquire)` pairs with `store(Release)`, and `is_initialized()` uses `load(Acquire)` to pair with `store(Release)`.

**Race window**: Two threads could both pass the `load(Acquire)` check and both enter validation. However, `lock_systems()` and `lock_registrations()` are `Mutex`es that serialize access, so only one thread validates at a time. The second thread redundantly re-validates identical state (idempotent), then stores `true` (also idempotent).

---

## S-004: Query Engine Sharing

### Baseline Requirement

The scheduler shares the `QueryEngine` via `Arc<QueryEngine>` with registered systems. Systems access it during `execute()` calls.

### Behavioral Impact

**None.** The `query_engine` field at [scheduler.rs:L35](scharnhorst_scheduler/src/scheduler.rs#L35) (`query_engine: Arc<QueryEngine>`) is unchanged. The QueryEngine's own internal locks are affected by Phase 1 (see query-engine delta spec), but the Scheduler's sharing pattern is unaffected.

**Evidence**: Systems access QueryEngine during `run_phase()` at [scheduler.rs:L240](scharnhorst_scheduler/src/scheduler.rs#L240):
```rust
let diffs = system.execute(&mut rng, &self.query_engine, phase, tick.as_u64())?;
```
The `&self.query_engine` borrow is a direct reference from the `Arc<QueryEngine>`. No scheduling-level lock protects this access — the QueryEngine's own internal locks handle concurrent access.

---

## S-005: Refresh Signal Protocol

### Baseline Requirement

After `atomic_commit()`, the scheduler broadcasts a refresh signal to all registered consumers. Consumers acknowledge before the next tick.

### Behavioral Impact — Phase 2: Broadcast Callback Snapshotting

**Current behavior**: `RefreshSignalBus::broadcast()` at [refresh_signal.rs:L88-L104](scharnhorst_scheduler/src/refresh_signal.rs#L88-L104):
```rust
pub fn broadcast(&self, tick: u64, generation: u64) -> SchedulerResult<()> {
    let consumers = self
        .consumers
        .lock()
        .map_err(|e| SchedulerError::Generic(...))?;
    consumers
        .iter()
        .map(|(name, cb)| {
            cb(tick, generation)
                .map_err(|e| SchedulerError::RefreshSignalFailed { consumer: name.clone(), ... })
        })
        .collect::<SchedulerResult<Vec<()>>>()
        .map(|_| ())
}
```
The `MutexGuard<HashMap<...>>` is held for the entire iteration + callback execution. If a callback takes significant time (e.g., Bevy's `SnapshotRefreshHandler::callback()` at [refresh_handler.rs:L92-L127](scharnhorst_bevy/src/refresh_handler.rs#L92-L127) which acquires the QueryEngine snapshot, increments generation, and refreshes the ViewModel), no other thread can register or unregister consumers.

**New behavior**: The consumer list is cloned under the lock, then the lock is released before iteration:
```rust
let callbacks: Vec<_> = {
    let consumers = self.consumers.lock()...;
    consumers.values().cloned().collect()
}; // lock released here
for cb in &callbacks {
    cb(tick, generation)?;
}
```

**Observable behavioral change**: Register/unregister calls that occur *during* a broadcast's callback execution now succeed immediately instead of blocking on the Mutex. However, the newly registered consumer will NOT receive the current broadcast, and the newly unregistered consumer WILL receive it. This is consistent with the old behavior — in the old code, register/unregister would block until after all callbacks completed, so the new registrant would not receive the current broadcast either.

**Test compatibility**: The existing test at [refresh_signal.rs:L149-L164](scharnhorst_scheduler/src/refresh_signal.rs#L149-L164) (`broadcast_receives_correct_params`) verifies that `(tick, gen)` values are correctly forwarded. The test at [refresh_signal.rs:L166-L182](scharnhorst_scheduler/src/refresh_signal.rs#L166-L182) (`broadcast_error_propagates`) verifies error propagation. The test at [refresh_signal.rs:L184-L201](scharnhorst_scheduler/src/refresh_signal.rs#L184-L201) (`unregister_stops_broadcast`) verifies that unregistered consumers don't receive subsequent broadcasts. All tests pass unchanged — none test the interleaving of register/unregister during broadcast.

**Evidence** (signature): `Scheduler::broadcast_refresh(&self, tick: Tick, state_hash: u64)` at [scheduler.rs:L260-L262](scharnhorst_scheduler/src/scheduler.rs#L260-L262). The second parameter — despite being named `generation` in the original code — receives `result.state_hash`, a content-based hash, not a monotonic counter. The spec refers to this parameter as `state_hash` to disambiguate from the Scheduler's own `generation` atomic and the ArrowStore's `generation` atomic. Neither the `RefreshSignalBus` callback (at [refresh_signal.rs:L68-L69](scharnhorst_scheduler/src/refresh_signal.rs#L68-L69)) nor the Bevy `RefreshHandler` (at [refresh_handler.rs:L68](scharnhorst_bevy/src/refresh_handler.rs#L68)) uses this parameter; the Bevy handler uses its own atomic `gen` counter.

---

## S-006: Debug Implementation

### Baseline Requirement

`Debug` for `Scheduler` shows system count, pending command count, current tick, and generation.

### Behavioral Impact

**No lock contention in Debug**: The old `Debug` impl at [scheduler.rs:L46-L68](scharnhorst_scheduler/src/scheduler.rs#L46-L68) acquired locks on `systems`, `pending_commands`, `current_tick`, and `generation` — each with `.lock().map(|g| ...).unwrap_or(default)`. The new Debug impl uses atomic loads for `current_tick` and `generation` (no lock contention). The `unwrap_or()` in the old code was a code smell: under `Mutex`, it silently produced zero/default values on poisoned locks. Under `AtomicU64`, there is no poison state — `load()` always succeeds.

**Evidence**: Lines [scheduler.rs:L58-L59](scharnhorst_scheduler/src/scheduler.rs#L58-L59):
```rust
let tick = self.current_tick.lock().map(|g| *g).unwrap_or(Tick::ZERO);
let gen = self.generation.lock().map(|g| *g).unwrap_or(0);
```
These become:
```rust
let tick = Tick(self.current_tick.load(Ordering::Relaxed));
let gen = self.generation.load(Ordering::Relaxed);
```
No unwrap, no poison, no silent default.

---

## S-007: Lock Helpers Removal

### Behavioral Impact

**Private API removal**: The lock helper methods at [scheduler.rs:L378-L420](scharnhorst_scheduler/src/scheduler.rs#L378-L420) — `lock_tick`, `lock_generation`, `lock_initialized` — are removed. These are `fn` private methods with no external callers. The remaining lock helpers (`lock_systems`, `lock_registrations`, `lock_commands`, `lock_journal`) are preserved because they protect complex multi-field state.

**Evidence**: `lock_systems` at [scheduler.rs:L378-L382](scharnhorst_scheduler/src/scheduler.rs#L378-L382), `lock_registrations` at [scheduler.rs:L384-L390](scharnhorst_scheduler/src/scheduler.rs#L384-L390), `lock_commands` at [scheduler.rs:L392-L396](scharnhorst_scheduler/src/scheduler.rs#L392-L396), and `lock_journal` at [scheduler.rs:L398-L402](scharnhorst_scheduler/src/scheduler.rs#L398-L402) are all preserved.

---

## New Invariants Introduced

1. **I-SCHED-TICK-ATOMIC**: `current_tick` and `generation` are atomically accessed with `Ordering::Relaxed`. Since all mutations occur on the single scheduler thread, there is no memory ordering requirement beyond atomicity. This `generation` counter is independent of the ArrowStore's `generation` counter (see [arrow-store spec](../arrow-store/spec.md) I-AS-GENERATION): the Scheduler increments in `atomic_commit()` after journal commit, while the ArrowStore increments at the start of `generate_snapshot()` called *within* the journal commit. The two have no happens-before relationship.

2. **I-SCHED-INIT-ACQUIRE**: `initialized` uses `Ordering::Acquire` via the `load`-then-`store` two-phase pattern (Amendment E / S-CA-001) — not via `compare_exchange`. The `Acquire` load on the fast-path check pairs with the `Release` store at the end of successful validation, ensuring all validation side-effects are visible to any thread that subsequently observes `is_initialized() == true`.

3. **I-SCHED-BROADCAST-SNAPSHOT**: `RefreshSignalBus::broadcast()` operates on a cloned snapshot of the consumer list, making it safe for `register`/`unregister` to run concurrently.

## Invariants Preserved

- Tick monotonicity (no lost increments, no duplicated ticks).
- Initialize idempotency (`initialize()` called N times with N≥1 produces exactly one validation pass).
- Broadcast error propagation (if any callback returns Err, broadcast returns immediately with `RefreshSignalFailed`).
- Phase execution order (all existing lock ordering for `systems`/`registrations`/`journal` preserved).

---

## Critical Analysis Amendments

This section documents issues discovered during deep review of the Scheduler atomic replacement design.

---

### S-CA-001: `initialize()` CAS — Permanent "Initialized" on Validation Failure (SIGNIFICANT)

**Location**: S-003 (Initialization)

**Problem**: The proposed `compare_exchange` pattern sets `initialized = true` BEFORE validation runs:

```rust
if self.initialized.compare_exchange(false, true, Acquire, Relaxed).is_err() {
    return Ok(());  // already initialized
}
// ... validation runs with initialized=TRUE already stored
```

If validation fails, `initialized` is already `true`. Subsequent calls return `Ok(())` immediately — the scheduler is permanently "initialized" with failed validation. The old Mutex design held the lock through validation AND the flag write, so a validation failure never set the flag.

The design argues this is safe because "initialize() is called once at application startup." However:
- The Bevy bridge may call `initialize()` from Bevy system setup, where `RefreshSignalBus::register()` runs concurrently (after Phase 2).
- Any future parallel startup code path triggers this bug.

**Impact**: Validation failures are masked permanently. The scheduler reports itself as initialized with potentially incorrect system registrations.

**Recommended Correction**: Replace the compare_exchange with a two-phase pattern:

```rust
pub fn initialize(&self) -> SchedulerResult<()> {
    // Fast path: already initialized
    if self.initialized.load(Ordering::Acquire) {
        return Ok(());
    }

    // Validation (must succeed before setting flag)
    let systems = self.lock_systems()?;
    let registrations = self.lock_registrations()?;
    // ... existing validation logic ...

    // Only now mark as initialized
    self.initialized.store(true, Ordering::Release);
    Ok(())
}
```

This preserves the idempotency contract while ensuring validation failures are never masked.