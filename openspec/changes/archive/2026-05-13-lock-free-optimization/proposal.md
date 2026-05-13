## Why

The codebase currently uses 24 Mutex/RwLock instances across 4 crates. Several of these protect single scalar values (u64, bool) that could be replaced with lock-free atomics.
The ArrowStore uses a single coarse-grained RwLock that serializes all reads and writes across all tables, creating a bottleneck as the number of tables and concurrent access patterns grow.
Additionally, the RefreshSignalBus holds a Mutex while invoking consumer callbacks, which can stall producer registration during long-running snapshot refresh operations.

## What Changes

- **Phase 1 — Atomic replacements**: Replace `Arc<Mutex<u64>>`, `Arc<Mutex<bool>>`, `Arc<Mutex<Tick>>` with `Arc<AtomicU64>` / `Arc<AtomicBool>` where the protected value is a single scalar. This covers 6 sites across Scheduler, QueryEngine, and Bevy bridge (SnapshotRefreshHandler, ViewModel).
- **Phase 2 — Broadcast callback snapshotting**: Clone the consumer callback list under the lock in `RefreshSignalBus::broadcast`, then invoke callbacks outside the lock.
- **Phase 3 — ArrowStore lock sharding**: Replace the single `Arc<RwLock<ArrowStoreInner>>` with per-field locking: `DashMap` for tables/snapshots, `AtomicU64` for counters, `RwLock` only for the rarely-mutated `TypeRegistry`.

All changes are **strictly internal**: no public API signatures change, no requirement-level behavior changes. Every function retains its existing return type and error contract.

## Capabilities

### New Capabilities
- None

### Modified Capabilities
- **arrow-store**: Internal locking restructured from single `RwLock<ArrowStoreInner>` to per-field `DashMap` + `AtomicU64` + per-field `RwLock` for `TypeRegistry`. All requirements and API signatures preserved; detailed behavioral changes in [specs/arrow-store/spec.md](specs/arrow-store/spec.md).
- **sim-scheduler**: `current_tick`, `generation`, `initialized` fields replaced with atomic types. `RefreshSignalBus::broadcast()` now snapshots consumer list before callback invocation. All requirements and API signatures preserved; detailed behavioral changes in [specs/sim-scheduler/spec.md](specs/sim-scheduler/spec.md).
- **query-engine**: `latest_tick` field replaced from `RwLock<Option<Tick>>` to `AtomicU64` with `u64::MAX` sentinel. All requirements and API signatures preserved; detailed behavioral changes in [specs/query-engine/spec.md](specs/query-engine/spec.md).
- **bevy-bridge**: `SnapshotRefreshHandler::state` split into two atomics; `ViewModel::generation` split from Mutex. All requirements and API signatures preserved; detailed behavioral changes in [specs/bevy-bridge/spec.md](specs/bevy-bridge/spec.md).

## Impact

- Affected crates: `scharnhorst_arrow_store`, `scharnhorst_scheduler`, `scharnhorst_query`, `scharnhorst_bevy`
- New dependency: `dashmap` (Phase 3, for `scharnhorst_arrow_store` only)
- Removed lock helper methods: `lock_tick`, `lock_generation`, `lock_initialized` (replaced by atomic ops)
- No breaking API changes. All public types and method signatures remain identical.
- Existing tests continue to pass; test assertions that depend on lock semantics (e.g. `journal_mut` returning `MutexGuard`) are updated to match new internal structures.