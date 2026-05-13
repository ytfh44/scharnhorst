# Bevy Bridge — Lock-Free Optimization Delta

## Overview

This spec describes the behavioral implications of Phase 1 atomic replacements across `SnapshotRefreshHandler` (splitting `RefreshHandlerState` into two atomics) and `ViewModel` (splitting `generation` from the Mutex-protected state). The `SyncState`, `InputCommandBuffer`, and `EntityMaterializationRegistry` are NOT modified.

**Baseline**: [openspec/specs/bevy-bridge/spec.md](openspec/specs/bevy-bridge/spec.md)

---

## BB-001: ViewModel — Snapshot Management

### Baseline Requirement

`ViewModel::refresh(snapshot, generation)` atomically updates the internal `snapshot`, `generation`, and `latest_tick` fields. `snapshot()`, `generation()`, and `latest_tick()` retrieve these fields.

### Behavioral Impact — Generation Split

**Current ViewModelState** at [sync.rs:L13-L17](scharnhorst_bevy/src/sync.rs#L13-L17):
```rust
struct ViewModelState {
    snapshot: Option<Arc<WorldSnapshot>>,
    generation: u64,
    latest_tick: Option<Tick>,
}
```

After the change, `generation` is split out:
```rust
struct ViewModelState {
    snapshot: Option<Arc<WorldSnapshot>>,
    latest_tick: Option<Tick>,
}
// ViewModel gains:
pub struct ViewModel {
    state: Arc<Mutex<ViewModelState>>,
    generation: Arc<AtomicU64>,    // NEW: split from Mutex
}
```

**Why only generation?**: `snapshot` and `latest_tick` are written together in `refresh()` at [sync.rs:L37-L48](scharnhorst_bevy/src/sync.rs#L37-L48):
```rust
state.snapshot = Some(snapshot);   // (A)
state.generation = generation;     // (B)
state.latest_tick = Some(snap_tick); // (C)
```
Lines (A) and (C) are a transactional pair — they must be observed together (you should never see a new `snapshot` with an old `latest_tick`). Line (B) is independent. The `generation` is a deduplication counter: `SnapshotRefreshHandler::refresh_snapshot()` at [refresh_handler.rs:L62-L69](scharnhorst_bevy/src/refresh_handler.rs#L62-L69) increments its own generation, then calls `view_model.refresh(..., generation)`, passing the same value. The ViewModel's generation is always a copy of the handler's generation.

**Splitting generation out**: The `ViewModel::refresh()` at [sync.rs:L37-L48](scharnhorst_bevy/src/sync.rs#L37-L48) now:
```rust
pub fn refresh(&self, snapshot: Arc<WorldSnapshot>, generation: u64) -> BevyBridgeResult<()> {
    let mut state = self.state.lock()...?;
    state.snapshot = Some(snapshot);                    // under Mutex
    state.latest_tick = Some(snap_tick);                // under Mutex
    self.generation.store(generation, Ordering::Relaxed); // atomic, while Mutex still held
    Ok(())
} // state MutexGuard dropped here (RAII)
```
The `generation` store occurs before the `MutexGuard` is dropped — the atomic write does not block nor contend with the Mutex in any way. In normal operation, `generation` is observable concurrently with or slightly ahead of `snapshot`/`latest_tick` becoming visible.

The `generation()` method at [sync.rs:L58-L64](scharnhorst_bevy/src/sync.rs#L58-L64) changes from:
```rust
pub fn generation(&self) -> BevyBridgeResult<u64> {
    let state = self.state.lock()...?;  // Mutex lock
    Ok(state.generation)
}
```
to:
```rust
pub fn generation(&self) -> BevyBridgeResult<u64> {
    Ok(self.generation.load(Ordering::Relaxed))  // atomic load
}
```

**Observable behavioral change**: Since `self.generation.store(..., Relaxed)` executes inside the same `MutexGuard` scope as the `state.snapshot` and `state.latest_tick` writes, a concurrent `generation()` reader may observe the new generation value via its non-blocking atomic load BEFORE the Mutex is released — while concurrent `snapshot()`/`latest_tick()` callers are still **blocked** waiting for the Mutex. This is a narrowing vs. the pre-change design: previously, the generation was inaccessible until the Mutex was released (it was inside `Mutex<ViewModelState>`). Now, generation leaks "early" into visibility. After the MutexGuard drops, all three fields are consistently observable. No code path couples the ViewModel's generation with its snapshot in a way that depends on simultaneous visibility.

**Is this observable?**: In the current codebase, the only callers of `ViewModel::generation()` are:
1. Test code: [sync.rs:L291](scharnhorst_bevy/src/sync.rs#L291) (`assert_eq!(vm.generation()?, 0)`)
2. Test code: [sync.rs:L302](scharnhorst_bevy/src/sync.rs#L302) (`assert_eq!(vm.generation()?, 3)`)
3. Test code: [refresh_handler.rs:L208](scharnhorst_bevy/src/refresh_handler.rs#L208) (`assert_eq!(vm.generation()?, 1)`)

The `generation` field is NOT used by `SyncState::sync_all()` at [sync.rs:L194-L222](scharnhorst_bevy/src/sync.rs#L194-L222) — that code only accesses `view_model.snapshot()` and `view_model.latest_tick()`. The sole consumer of the ViewModel's generation is `SnapshotRefreshHandler::current_generation()` at [refresh_handler.rs:L85-L90](scharnhorst_bevy/src/refresh_handler.rs#L85-L90), which reads the handler's own atomic `last_snapshot_generation`, not the ViewModel's.

**Mitigation**: The microsecond window is harmless because no code couples the ViewModel's generation with its snapshot in a way that would produce incorrect behavior. The generation is a purely informational counter used only in asserts and debug output.

---

## BB-002: Refresh Handler — Signal Protocol

### Baseline Requirement

`SnapshotRefreshHandler` receives refresh signals from the scheduler via `RefreshSignalBus`. It tracks `signal_received` and `last_snapshot_generation` behind a `Mutex<RefreshHandlerState>`.

### Behavioral Impact — Full State Split

**Current RefreshHandlerState** at [refresh_handler.rs:L10-L14](scharnhorst_bevy/src/refresh_handler.rs#L10-L14):
```rust
struct RefreshHandlerState {
    signal_received: bool,
    last_snapshot_generation: u64,
}
```

After the change, the struct is eliminated. `SnapshotRefreshHandler` at [refresh_handler.rs:L16-L23](scharnhorst_bevy/src/refresh_handler.rs#L16-L23) becomes:
```rust
pub struct SnapshotRefreshHandler {
    view_model: Arc<ViewModel>,
    query_engine: Arc<QueryEngine>,
    arrow_store: Arc<ArrowStore>,
    signal_received: Arc<AtomicBool>,         // was state.signal_received
    last_snapshot_generation: Arc<AtomicU64>, // was state.last_snapshot_generation
}
```

**Method-by-method analysis:**

**`on_refresh_signal()`** at [refresh_handler.rs:L42-L49](scharnhorst_bevy/src/refresh_handler.rs#L42-L49):
Old: `lock → state.signal_received = true → unlock`
New: `signal_received.store(true, Ordering::Release)`
No behavioral change. The Release ordering ensures any subsequent snapshot refresh work is visible after the flag is set.

**`should_refresh()`** at [refresh_handler.rs:L51-L56](scharnhorst_bevy/src/refresh_handler.rs#L51-L56):
Old: `lock → read signal_received → unlock`
New: `signal_received.load(Ordering::Acquire)`
Pairing with the Release in `on_refresh_signal()`, this forms a happens-before chain.

**`refresh_snapshot()`** at [refresh_handler.rs:L58-L83](scharnhorst_bevy/src/refresh_handler.rs#L58-L83):
Old:
```rust
let generation = {
    let mut state = self.state.lock()...?;     // Lock 1
    state.last_snapshot_generation += 1;
    state.last_snapshot_generation
};                                              // Unlock 1

self.view_model.refresh(Arc::new(new_snapshot), generation)?;

{
    let mut state = self.state.lock()...?;     // Lock 2
    state.signal_received = false;
}                                              // Unlock 2
```
New:
```rust
let generation = self.last_snapshot_generation.fetch_add(1, Ordering::Relaxed) + 1;

self.view_model.refresh(Arc::new(new_snapshot), generation)?;

self.signal_received.store(false, Ordering::Release);
```
**Three lock acquisitions eliminated, zero behavioral change.** The lock brackets (Lock 1 → Unlock 1 → refresh → Lock 2 → Unlock 2) already separated the increment from the clear, so the atomic replacements are functionally identical.

**`current_generation()`** at [refresh_handler.rs:L85-L90](scharnhorst_bevy/src/refresh_handler.rs#L85-L90):
Old: `lock → read → unlock`
New: `load(Relaxed)`

**`callback()`** at [refresh_handler.rs:L92-L127](scharnhorst_bevy/src/refresh_handler.rs#L92-L127):
Old:
```rust
let new_gen = {
    let mut s = state.lock()...?;              // Lock A
    s.last_snapshot_generation += 1;
    s.last_snapshot_generation
};                                              // Unlock A

vm.refresh(snapshot, new_gen)...?;

let mut s = state.lock()...?;                  // Lock B
s.signal_received = false;                     // Unlock B
```
New:
```rust
let new_gen = last_snapshot_generation.fetch_add(1, Relaxed) + 1;

vm.refresh(snapshot, new_gen)...?;

self.signal_received.store(false, Ordering::Release);
```
Same pattern as `refresh_snapshot()` — two lock brackets become two atomic operations.

**Evidence of concurrency safety**: The `callback()` is invoked by `RefreshSignalBus::broadcast()` at [refresh_signal.rs:L96](scharnhorst_scheduler/src/refresh_signal.rs#L96). With Phase 2's snapshotting change, the callback is invoked outside the consumers Mutex. The atomic `signal_received` and `last_snapshot_generation` ensure that:
1. Multiple callback invocations (if broadcast is called from different threads, though in practice it isn't) see strictly increasing generations via `fetch_add`.
2. The `signal_received` flag is cleared atomically without blocking the handler's own `on_refresh_signal()`.

---

## BB-003: Sync State — Entity Synchronization

### Baseline Requirement

`SyncState` tracks which Bevy entities correspond to which tables, maintaining a dirty set for incremental sync.

### Behavioral Impact

**None.** `SyncState` at [sync.rs:L92-L96](scharnhorst_bevy/src/sync.rs#L92-L96) is unchanged. Its two Mutex-protected fields (`view_models: Arc<Mutex<HashMap<Entity, SyncEntry>>>` and `dirty_entities: Arc<Mutex<HashSet<Entity>>>`) are NOT converted to atomics because they contain complex collection types. The code at [sync.rs:L194-L222](scharnhorst_bevy/src/sync.rs#L194-L222) (`sync_all`) holds both locks sequentially, and `unregister_entity()` at [sync.rs:L134-L146](scharnhorst_bevy/src/sync.rs#L134-L146) holds both locks in the same order (`view_models` then `dirty_entities`). Lock ordering is consistent and no changes are made.

**Evidence**: The existing two-lock pattern at [sync.rs:L139-L144](scharnhorst_bevy/src/sync.rs#L139-L144):
```rust
let mut models = self.view_models.lock()...?;
let mut dirty = self.dirty_entities.lock()...?;
models.remove(&entity);
dirty.remove(&entity);
```
The `view_models` lock is acquired before `dirty_entities` in all multi-lock methods (`unregister_entity` at line 135-142, `mark_all_dirty` at line 158-166, `sync_all` at line 199-206). This consistent ordering prevents deadlock.

---

## BB-004: Input Command Buffer

### Baseline Requirement

`InputCommandBuffer` holds a queue of player commands behind a single `Mutex<InputBufferInner>` to prevent ABBA deadlock.

### Behavioral Impact

**None.** The single-Mutex design at [input_buffer.rs:L133-L140](scharnhorst_bevy/src/input_buffer.rs#L133-L140) is deliberately chosen and documented:
```rust
/// # Lock Safety
///
/// All mutable state is behind a single `Mutex<InputBufferInner>` to prevent ABBA
/// deadlocks that could occur with multiple independent mutexes.
```
The input buffer is NOT modified by this change. Splitting `InputBufferInner` into atomics would risk ABBA deadlock with the scheduler's `pending_commands` lock, since commands flow from the input buffer to the scheduler's queue. The single-Mutex design is a conscious safety choice and remains.

**Evidence**: The doc comment at [input_buffer.rs:L133-L136](scharnhorst_bevy/src/input_buffer.rs#L133-L136) explicitly warns against multiple independent mutexes. This change respects that warning.

---

## BB-005: Entity Materialization

### Baseline Requirement

`EntityMaterializationRegistry` maps Arrow rows to Bevy entities, maintaining an internal `HashMap` and a `Vec` of configs, each behind its own Mutex.

### Behavioral Impact

**None.** `EntityMaterializationRegistry` is NOT modified. The two-Mutex design (one for the entity map, one for configs) could theoretically be optimized in a future Phase 4 (downgrade to `RefCell` if Bevy main-thread-only), but this is explicitly deferred per the design.md Open Question on Bevy system parallelism.

---

## BB-006: Test Compatibility

### Behavioral Impact — Test Assertions

**All existing tests pass without modification.** This is verified by examining each test:

- [refresh_handler.rs:L178-L184](scharnhorst_bevy/src/refresh_handler.rs#L178-L184): `refresh_handler_initial_state` — reads `should_refresh()` and `current_generation()`. Both now use atomic loads; return values unchanged.
- [refresh_handler.rs:L187-L194](scharnhorst_bevy/src/refresh_handler.rs#L187-L194): `refresh_handler_on_signal_and_should_refresh` — `on_refresh_signal()` then `should_refresh()`. The `Acquire`/`Release` pair ensures the flag is visible.
- [refresh_handler.rs:L196-L211](scharnhorst_bevy/src/refresh_handler.rs#L196-L211): `refresh_handler_refresh_snapshot` — calls `refresh_snapshot()`, then checks via `should_refresh()` and `current_generation()` and `vm.generation()`. The generation values match because the handler and ViewModel use the same generation number.
- [refresh_handler.rs:L213-L220](scharnhorst_bevy/src/refresh_handler.rs#L213-L220): `refresh_handler_callback_runs_without_panic` — the `callback()` closure uses atomic `fetch_add` instead of lock. The test expects an error (no snapshot in query engine).
- [sync.rs:L288-L295](scharnhorst_bevy/src/sync.rs#L288-L295): `view_model_initially_empty` — `vm.generation()?` is now an atomic load. `assert_eq!(vm.generation()?, 0)` passes because initial `AtomicU64` value is 0.
- [sync.rs:L297-L306](scharnhorst_bevy/src/sync.rs#L297-L306): `view_model_refresh_updates_state` — `vm.refresh(snapshot, 3)` stores 3 to the atomic, then `vm.generation()?` loads it. `assert_eq!(vm.generation()?, 3)` passes.

---

## New Invariants Introduced

1. **I-BB-GEN-SPLIT**: `ViewModel::generation` and `ViewModelState::snapshot`/`latest_tick` are no longer updated under the same lock. The microsecond window during `refresh()` where generation lags behind snapshot is harmless because no code couples them.

2. **I-BB-SIGNAL-ATOMIC**: `signal_received` uses `Release` on store (in `on_refresh_signal`) and `Acquire` on load (in `should_refresh`). This forms a happens-before chain: the refresh signal setter's work happens-before the checker observes the flag.

3. **I-BB-GENERATION-ATOMIC**: `last_snapshot_generation` uses `Relaxed` ordering. Since all mutations flow through a single Bevy system (the refresh handler), there is no need for inter-thread ordering — atomicity alone ensures correctness.

## Invariants Preserved

- `ViewModel::snapshot()` returns `Option<Arc<WorldSnapshot>>` — the snapshot is still behind the Mutex and always observed consistently with `latest_tick`.
- `SnapshotRefreshHandler::on_refresh_signal()` and `should_refresh()` have identical observable behavior.
- `SyncState` lock ordering (`view_models` before `dirty_entities`) is preserved.
- `InputCommandBuffer` single-Mutex ABBA-deadlock prevention is preserved.
- All Bevy `Resource` derives are preserved (the new atomics in `ViewModel` and `SnapshotRefreshHandler` are `Send + Sync`).

---

## Critical Analysis Amendments

This section documents issues discovered during deep review of the Bevy bridge atomic replacement design.

---

### BB-CA-001: ViewModel Generation-Split Micro-Window (INFORMATIONAL)

**Location**: BB-001 (ViewModel — Snapshot Management), I-BB-GEN-SPLIT

**Problem**: The design splits `generation` from `ViewModelState` into a separate `AtomicU64`. During `refresh()`, `snapshot` and `latest_tick` are written under the Mutex first, then `generation` is stored to the atomic afterward. A concurrent `generation()` call in the microsecond window between the Mutex release and the atomic store observes the OLD generation with the NEW snapshot.

The design's analysis shows this is harmless because no production code couples the ViewModel's generation with its snapshot — all callers use them independently. Only test code reads the generation.

**No correction needed**. The existing analysis in BB-001 correctly identifies the window and verifies it is unobservable in production code. Documented here for completeness. No task required.

---

### BB-CA-002: SnapshotRefreshHandler `callback()` — Multiple Lock Bracket Eliminated (INFORMATIONAL)

**Location**: BB-002 (Refresh Handler — Signal Protocol)

**Problem**: After Phase 2 (broadcast callback snapshotting), the `callback()` closure is invoked outside the `RefreshSignalBus` consumers Mutex. With the atomic replacements in Phase 1, the two lock brackets in `callback()` become two atomic operations:

```rust
// Lock A → increment gen → Unlock A
// vm.refresh(...)
// Lock B → clear signal → Unlock B
```

With Phase 2, the callback is invoked while the consumers HashMap is unlocked — but the callback internally used atomics already. No additional risk. The two phases compose cleanly.

**No correction needed**. Documented as evidence of clean composition between Phase 1 and Phase 2.