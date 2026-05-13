## Context

The Scharnhorst engine uses 24 synchronization primitives (Mutex, RwLock) across 4 crates. A deep audit revealed that many of these protect single scalar values (u64, bool, Tick) that are trivially replaceable with lock-free atomics. Others exhibit granularity problems: the ArrowStore uses a single coarse-grained RwLock for all state, and the RefreshSignalBus holds a lock while invoking external callbacks.

The project enforces strict error handling (no `unwrap`/`expect`/`panic` in library code) and API stability. All optimizations must preserve the existing public API surface, error types, and behavioral contracts.

## Goals / Non-Goals

**Goals:**
- Replace `Mutex<u64>`, `Mutex<bool>`, `Mutex<Tick>` with `AtomicU64`/`AtomicBool` wherever the value is a single scalar and all mutations are simple loads/stores/fetch_adds.
- Reduce RefreshSignalBus lock hold time by snapshotting the callback list before iteration.
- Shard ArrowStore's Big Lock into per-field locking to enable concurrent reads across different tables and concurrent snapshot generation with table mutations.
- Preserve all public API signatures, return types, error types, and behavioral semantics exactly.

**Non-Goals:**
- Changing the ownership model of shared state (e.g., removing `Arc` from evaluator's QueryEngine/Journal references).
- Lock-free data structures for complex types (e.g., HashMap replacement with concurrent hash maps beyond the ArrowStore sharding).
- Replacing Bevy ECS internal synchronization (Bevy manages its own threading model).
- Atomic replacement for `Mutex<Journal>` or `Mutex<HashMap<...>>` — these contain multi-field state that requires transactional consistency.
- Removing the `RwLock` around `QueryEngine::view_cache` — this is a complex HashMap whose read/write patterns benefit from the RwLock's reader-writer semantics. Potential RCU-based patterns (evmap, left-right) are deferred to a future investigation.

## Decisions

### Decision 1: Use `AtomicU64` for Tick/u64, `AtomicBool` for bool

**Rationale**: `Tick` is a `Copy` newtype over `u64` (8 bytes). `AtomicU64` is also 8 bytes with identical alignment. The operations needed — load, store, fetch_add — map directly to `Tick::as_u64()`, `Tick(u64)`, and `Tick::next()`. No `Ordering::SeqCst` is required since the scheduler executes phases sequentially; `Ordering::Relaxed` suffices for tick/generation counters and `Ordering::Release`/`Ordering::Acquire` for signal flags.

**Alternatives considered**:
- `AtomicCell<Tick>` from `crossbeam`: Adds a dependency for negligible benefit over raw `AtomicU64`.
- Keep Mutex but use `try_lock` with fallback: Does not eliminate contention, just fails faster.
- `parking_lot::Mutex`: Still a lock, still has overhead. Atomics are strictly faster.

**Memory ordering justification**:

| Field | Operation | Ordering | Reasoning |
|-------|-----------|----------|-----------|
| `current_tick` | load | Relaxed | Tick is monotonic; each phase runs sequentially, no happens-before needed with other fields |
| `current_tick` | fetch_add | Relaxed | Same — sequential phase execution means no concurrent tick increments |
| `generation` | load | Relaxed | Read-only informational; generation number is advisory |
| `generation` | fetch_add | Relaxed | Only incremented in atomic_commit, no concurrent readers depend on exact value ordering |
| `initialized` | compare_exchange | Acquire/Relaxed | Acquire needed so validation writes happen-before subsequent readers of `is_initialized` |
| `signal_received` | store(false) | Release | Release so prior snapshot refresh happens-before any thread that reads signal_received==false |

### Decision 2: RefreshSignalBus broadcasts outside the lock

**Rationale**: The current `broadcast` holds `Mutex<HashMap<...>>` while iterating and calling each consumer callback. If a callback is slow (e.g., Bevy snapshot refresh involving WorldSnapshot copy + ViewModel update), no other thread can register or unregister consumers during that time. By cloning the callback list under the lock and releasing it before invocation, we allow concurrent register/unregister.

**Trade-off**: Register/unregister changes made during a broadcast will not be observed by that broadcast cycle. This is acceptable because:
1. The previous behavior also guaranteed a stable snapshot (lock held for entire iteration).
2. Register/unregister is a rare operation (typically only at scheduler initialization).
3. The same consumer cannot both register AND expect to receive the current broadcast — there is no happens-before relationship either way.

### Decision 3: Use `DashMap` for ArrowStore table/snapshot storage

**Rationale**: `DashMap<String, Arc<RwLock<VersionedTable>>>` provides per-shard locking, allowing concurrent operations on different tables. `VersionedTable` itself is wrapped in `Arc<RwLock<...>>` because it is not `Send+Sync` and its fields are publicly mutable.

**Schema of the new ArrowStore** (all fields wrapped in `Arc<...>` because `ArrowStore` derives `Clone` and must share state across clones):

```
ArrowStore {
    tables:        Arc<DashMap<String, Arc<RwLock<VersionedTable>>>>,
    table_id_map:  Arc<DashMap<TableId, String>>,
    snapshots:     Arc<DashMap<Tick, Arc<WorldSnapshot>>>,
    next_table_id: Arc<AtomicU64>,
    generation:    Arc<AtomicU64>,
    type_registry: Arc<RwLock<TypeRegistry>>,     // rarely mutated, mostly reads post-init
    create_drop_lock: Arc<Mutex<()>>,             // serializes create_table/drop_table pairs
}
```

**Key design constraints**:
- `create_table`: Must atomically allocate `next_table_id`, insert into `tables`, insert into `table_id_map`. With separate structures, we use `fetch_add` for the ID and two sequential `DashMap` inserts, both serialized by `create_drop_lock: Mutex<()>` (per Amendment C). The `fetch_add` is deliberately placed BEFORE the lock to avoid holding the mutex across an atomic instruction; skipped IDs on duplicate-name rejection are harmless. Between the two DashMap inserts (tables, then table_id_map), a caller holding `create_drop_lock` observes both atomically.
- `drop_table`: Must remove from both `tables` and `table_id_map`. Both removals are serialized by `create_drop_lock: Mutex<()>` (per Amendment C). Within the lock, the code iterates `table_id_map` to locate the matching `TableId`, removes the `table_id_map` entry, then removes the table from `tables`. Since both steps are under one lock, the removal order is unobservable — table and ID mapping disappear atomically.
- `generate_snapshot`: Must snapshot ALL tables consistently. Since we now have per-table locks, we iterate `tables` entries, acquiring each table's read lock sequentially. A table modified during iteration will be snapshotted either before or after the modification — both outcomes are valid because a snapshot is a point-in-time view and concurrent writes have no defined ordering relative to the snapshot.
- `load_checkpoint`: The current code inlines snapshot generation to avoid RwLock re-entrancy (cannot call `generate_snapshot` while holding the outer write lock). With per-table locking, this constraint disappears — `load_checkpoint` can call `generate_snapshot` normally.

**Alternatives considered**:
- `std::sync::RwLock<HashMap<...>>` with manual sharding (e.g., 16-way striped lock): More code, hard to tune shard count, no proven benefit over DashMap's built-in sharding.
- `evmap` for snapshots: Supports multi-version concurrency control but overkill for a write-rarely-read-often workload. DashMap is simpler.
- Keep single RwLock but make it `parking_lot::RwLock`: Slightly faster lock implementation but does not address the fundamental contention problem of serializing all table access.

### Decision 4: SnapshotRefreshHandler two-field state split into two atomics

**Rationale**: `RefreshHandlerState { signal_received: bool, last_snapshot_generation: u64 }` has two independent fields. The current code pattern already demonstrates they are accessed independently:
1. `on_refresh_signal`: lock, set `signal_received = true`, unlock.
2. `refresh_snapshot`: lock, increment `last_snapshot_generation`, unlock, refresh ViewModel, lock, set `signal_received = false`, unlock.
3. `callback()` (closure): lock, increment gen, unlock, refresh ViewModel, lock, clear signal.

Splitting into `signal_received: AtomicBool` + `last_snapshot_generation: AtomicU64` eliminates all three lock acquisitions, replacing them with atomic stores/loads. The interleaving of signal_received and generation updates has no ordering dependency — signal_received is a notification flag, generation is a monotonic counter for deduplication.

### Decision 5: ViewModel generation split from snapshot

**Rationale**: `ViewModelState { snapshot: Option<Arc<WorldSnapshot>>, generation: u64, latest_tick: Option<Tick> }` — the `generation` field is queried independently by `generation()`. Splitting it to `AtomicU64` removes the lock from that access path. The `snapshot` and `latest_tick` fields remain under the Mutex since they are updated atomically together in `refresh()`.

## Risks / Trade-offs

### Risk 1: DashMap iteration is not snapshot-consistent
DashMap's `.iter()` sees entries as they exist at the moment of visitation. During `generate_snapshot` iteration, a concurrent `create_table` or `drop_table` may cause the new table to be included or excluded non-deterministically.

**Mitigation**: `generate_snapshot` is called from `journal.commit()`, which runs within the scheduler's `atomic_commit()` phase. During commit, no other systems are running (sequential phases), so no concurrent mutations occur. The per-table locking primarily benefits read-heavy workloads outside the commit cycle (e.g., inspector queries, AI evaluation). Within a tick boundary, the sequential phase execution guarantees single-writer semantics.

### Risk 2: Atomic initialization changes the `initialize()` idempotency contract

**Resolution**: Amendment E replaced the proposed `compare_exchange` with a two-phase `load(Acquire)` → validation → `store(Release)` pattern. This preserves the idempotency contract while ensuring validation failures are never masked: if validation fails, `initialized` is never set to `true`, and subsequent callers re-attempt validation.

**Residual risk**: The two-phase pattern has a narrow window where two threads both pass the `load(Acquire)` check and both enter validation under Mutex serialization. The second thread redundantly re-validates the same state (which is idempotent), then stores `true` (also idempotent). This is harmless under the current single-threaded tick lifecycle.

### Risk 3: `DashMap` adds a new dependency
**Mitigation**: `dashmap` is a well-established crate (5M+ downloads) with minimal dependencies (`hashbrown`, `lock_api`). It is actively maintained and used in production systems. The alternative of hand-rolling a sharded `RwLock<HashMap>` with array-of-locks introduces more code and potential bugs than adding a trusted dependency.

### Risk 4: Removal of `lock_tick`, `lock_generation`, `lock_initialized` affects test code
Tests that directly call these methods or depend on `MutexGuard` types will break.

**Mitigation**: These methods are `fn lock_*(&self)` — private helpers. No external crate can call them. Internal test code in `scheduler.rs` uses only public APIs (`current_tick()`, `advance_tick()`, etc.). The `journal_mut()` test verifies it returns `MutexGuard<Journal>`, which is unaffected (journal lock is unchanged).

### Risk 5: Ordering between `Ordering::Relaxed` ticks and `Ordering::Acquire` initialized
If a consumer thread reads `is_initialized() == true` (Acquire) but then reads `current_tick` (Relaxed), there is no formal guarantee the tick value reflects the state at initialization time.

**Mitigation**: The scheduler design is fundamentally single-threaded for tick lifecycle. The `Arc` sharing exists for Bevy's system parameter injection, not for concurrent tick execution. The atomics' primary purpose is safety, not inter-thread signaling. Relaxed ordering is correct for all tick/generation counters because they are only modified from the single scheduler thread.

## Migration Plan

1. Phase 1 (atomic replacements): Each site is a self-contained change within a single file. No migration needed — the changes are invisible to callers.
2. Phase 2 (broadcast snapshotting): Single-function change. No migration needed.
3. Phase 3 (ArrowStore sharding): This requires coordinated changes to `store.rs` (struct definition, all methods). Since the public API is unchanged, no caller migration is needed. The `DashMap` dependency is added to `scharnhorst_arrow_store/Cargo.toml`.

**Rollback**: Each phase is independently revertible via `git revert`. Phases do not depend on each other.

## Open Questions

- Should `QueryEngine::latest_tick` use a sentinel `u64::MAX` for `None`, or keep `RwLock<Option<Tick>>`? **Resolved**: The sentinel approach is used, with `Tick::MAX = Tick(u64::MAX - 1)` reserving `u64::MAX` at the type level. `Tick::next()` uses `saturating_add(1)`, so `Tick::MAX.next()` saturates to the sentinel. At 1 billion ticks/sec, reaching Tick::MAX requires ~585 years. A `debug_assert!` in `ingest_snapshot` guards against accidental sentinel ingestion.
- Should `PrefetchCache` (currently single-threaded HashMap, no lock) be made concurrent if rules evaluation is parallelized in the future? This is a separate concern tracked as a potential future optimization, not part of this change.
- Can Bevy's `SyncState` and `MaterializationRegistry` Mutexes be downgraded to `RefCell`? Requires profiling Bevy's system parallelism to determine if these resources are ever accessed from multiple systems concurrently. Deferred to Phase 4 investigation.

---

## Critical Analysis Amendments

This section documents issues surfaced during a deep critical review of the design. Each amendment is categorized by severity and includes a recommended design correction before implementation proceeds.

---

### Amendment A (CRITICAL — APPLIED): Tick Sentinel Collision — Silent Data Corruption

**Location**: Decision 1 (AtomicU64 for Tick), QueryEngine Phase 1

**Problem**: `u64::MAX` is reserved as the None sentinel for `latest_tick`. `Tick` is a public newtype over `u64` with `pub u64` field access. Any code path — test code, checkpoint deserialization, deterministic replay — can construct `Tick(u64::MAX)`. If ingested via `ingest_snapshot`, the atomic store of `u64::MAX` is indistinguishable from `None`.

**Applied correction**:
1. `Tick::MAX = Tick(u64::MAX - 1)` added as public const at [id.rs:L19](scharnhorst_core/src/id.rs#L19).
2. `debug_assert!(tick.as_u64() != u64::MAX)` added at [engine.rs:L152](scharnhorst_query/src/engine.rs#L152) in `ingest_snapshot`.
3. Sentinel invariant documented on `latest_tick`.

---

### Amendment B (CRITICAL — APPLIED): `ingest_snapshot` Ordering — Stated Invariant Is Fragile

**Location**: QE-003, I-QE-TICK-ORDERING

**Problem**: The stated invariant claims:
> "Any reader observing tick T also sees all view_cache insertions performed prior to T's store."

This is achieved via `store(Release)` in `ingest_snapshot` paired with `load(Acquire)` in `latest_tick()`. However, the invariant **only holds if the reader always loads `latest_tick` BEFORE reading `view_cache`**. A reader that reads `view_cache` first (under its own read lock), then reads `latest_tick`, can observe:

1. Reader: acquires `view_cache.read()` → sees old data (writer hasn't started or is mid-write)
2. Writer: completes both `cache.insert()` and `latest_tick.store(Release)`
3. Reader: `latest_tick.load(Acquire)` → sees new tick T
4. Reader: uses old cache data under the belief it corresponds to tick T

**Impact**: Stale reads — a reader can cache data from tick N-1 and associate it with tick N.

**Recommended Correction**:

1. Add a documentation block on `ingest_snapshot`:
   ```
   /// ORDERING INVARIANT: Readers MUST load `latest_tick()` BEFORE
   /// acquiring the `view_cache` read lock. Violating this order may
   /// cause stale cache data to be attributed to a newer tick.
   ```
2. Add a comment on `latest_tick()`:
   ```
   /// NOTE: The (Release) store in ingest_snapshot pairs with this
   /// (Acquire) load. To maintain the ordering invariant, call this
   /// method BEFORE reading view_cache.
   ```
3. In `lookup_row()` and all other reader sites, verify the ordering is correct.

---

### Amendment C (CRITICAL — APPLIED): `create_table`/`drop_table` Race — Table-ID Map Orphans

**Location**: Decision 3 (DashMap), AS-001 / AS-002

**Problem**: With per-field DashMaps, `create_table` does two sequential inserts (tables, then table_id_map). `drop_table` does two sequential removes (tables first, then table_id_map, per the new design). A concurrent `create_table("X")` and `drop_table("X")` can interleave:

| Step | create_table thread          | drop_table thread              |
|------|------------------------------|--------------------------------|
| 1    | `tables.insert("X", ...)`    |                                |
| 2    |                              | `tables.remove("X")` — found! |
| 3    |                              | `table_id_map.remove(id)` — no entry yet, silently skipped |
| 4    | `table_id_map.insert(id, "X")` |                              |

Result: orphan in `table_id_map` with no corresponding table. The design asserts "no code path iterates table_id_map independently" — but `drop_table` itself previously iterated it, and while the new design eliminates that iteration, the orphan still exists and accumulates garbage.

**Impact**: Accumulated garbage entries in `table_id_map` that are never cleaned up. If a future code path iterates `table_id_map` (e.g., debugging, inspection), the stale entries may cause confusion or errors.

**Recommended Correction**: Wrap the two inserts (or two removes) in a single short-lived critical section:

```rust
let id_val = self.next_table_id.fetch_add(1, Relayed);
let id = TableId::new(id_val);
// Both inserts under a single std::sync::Mutex for correctness
// (DashMap by itself cannot atomically insert into two maps)
let mut guard = self.create_drop_lock.lock()...?;
self.tables.insert(spec.name.clone(), ...);
self.table_id_map.insert(id, spec.name.clone());
drop(guard);
```

Add a single `Mutex<()>` or `RwLock<()>` to the ArrowStore struct, used only for the create/drop pair. Contention is negligible (create/drop are rare, and the lock is held for microseconds).

---

### Amendment D (SIGNIFICANT — APPLIED): `generate_snapshot` Is NOT Point-in-Time

**Location**: Decision 3 (DashMap), AS-003

**Problem**: The old Big Lock guaranteed that all tables in a snapshot reflect a single frozen point in time. With per-table iteration in `generate_snapshot`, table A is snapshotted at time T1 and table B at time T2 (T2 > T1). A concurrent modification to table B between T1 and T2 means the snapshot contains table A at T1 and table B at T2. This violates cross-table consistency: a foreign key in table A might reference a row in table B that was deleted between T1 and T2.

The design acknowledges this but relies on "sequential phase execution prevents concurrent writers" — which is true only for the scheduler's commit path. The design explicitly states "the per-table locking primarily benefits non-tick-boundary reads (inspector queries, AI evaluation)." If an inspector or AI evaluator calls `generate_snapshot` while a concurrent writer exists, the snapshot is NOT point-in-time.

**Impact**: Cross-table foreign-key inconsistency in snapshots generated outside the commit path.

**Recommended Correction**: Add a documentation-only guard:

1. Document on `generate_snapshot` that it must not be called concurrently with mutating operations when cross-table consistency is required.
2. Keep the current behavior for the commit path (which is single-threaded and safe).
3. Optionally: if concurrent snapshot generation becomes necessary in the future, add an `Arc<RwLock<()>>` snapshot_generation_lock that serializes generate_snapshot with any concurrent create_table/drop_table/apply_diffs.

---

### Amendment E (SIGNIFICANT — APPLIED): `initialize()` CAS — Permanent Poison on Validation Failure

**Location**: Decision 1 (atomic replacements), S-003

**Problem**: The design proposes:

```rust
if self.initialized.compare_exchange(false, true, Acquire, Relaxed).is_err() {
    return Ok(());  // already initialized
}
// ... validation ...
// validation runs with initialized=TRUE already stored
```

If validation fails (returns Err), `initialized` is already `true`. Subsequent calls return `Ok(())` immediately — the scheduler is permanently "initialized" with failed validation. The old Mutex design held the lock through validation AND the flag write, so a validation failure never set the flag.

The design argues this is safe because "`initialize()` is called once at application startup before any ticks." However:
- The Bevy bridge may call `initialize()` from Bevy system setup, where `RefreshSignalBus::register()` runs concurrently (after Phase 2's snapshotting change).
- Any future code path that parallelizes startup triggers this bug.

**Impact**: A validation failure is masked permanently. The scheduler reports itself as initialized but the simulation may have incorrect system registrations or undetected write conflicts.

**Recommended Correction**: Restructure the CAS to only set `initialized = true` AFTER validation succeeds:

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

Remove the compare_exchange entirely. Use a two-phase pattern: `load(Acquire)` for the fast-path check, then full validation, then `store(Release)` after success. The `Release` ensures all validation writes are visible to readers after the flag is set.

---

### Amendment F (SIGNIFICANT — APPLIED): `truncate_before` — Snapshot/Version Interleaving

**Location**: Decision 3 (DashMap)

**Problem**: `truncate_before` does two steps: (1) retain snapshots with `t >= threshold`, (2) iterate tables and retain versions with `t >= threshold`. With per-table locking, a concurrent `generate_snapshot(T_new)` between steps 1 and 2 can insert a snapshot at T_new (which is retained because step 1 already passed that shard, or the new entry lands in a different shard) AND a table version at T_new (which is removed because step 2 runs after the version was inserted and T_new < threshold). The snapshot then has a dangling reference to a deleted version.

**Impact**: Corrupted snapshot referencing non-existent versions.

**Recommended Correction**: Document that `truncate_before` must not run concurrently with `generate_snapshot`. In practice, this is already enforced by the sequential phase design, but should be explicitly stated on the `truncate_before` method.

---

### Amendment G (MODERATE — PRESCRIBED): `apply_diffs` Multi-Step Read-Then-Write Not Atomic

**Location**: Decision 3 (DashMap), AS-008

**Problem**: The current `ArrowStore` diff application methods do multiple `self.tables.get(table_name)` calls for a single diff operation. For example, `apply_update` does:

1. `self.tables.get(table_name)` — read position_map → row position
2. `self.tables.get(table_name)` — read versions → locate tick
3. `self.tables.get(table_name)` — read batch → build patch
4. `self.patch_inner(table_name)` — write to table

Each DashMap `.get()` followed by `.read()`/`.write()` on the `Arc<RwLock<VersionedTable>>` acquires and releases the lock. Between steps 1-4, another thread's operation on the SAME table can interleave — modifying position_map or versions — producing incorrect diff application results.

**Status**: Applied. The restructured design is specified in AS-008 and AS-CA-004. All amendments (A-H) have been applied to the codebase. `ArrowStore::apply_diffs()` dispatches each diff by acquiring the per-table write lock once, then delegating to standalone functions (`apply_update_to_table`, `apply_delete_to_table`, etc.) that accept `&mut VersionedTable` + `&TypeRegistry`. This ensures each diff's read-modify-write cycle happens atomically within a single held write lock.

---

### Amendment H (MODERATE — APPLIED): DashMap Iteration Non-Determinism in Metadata Methods

**Location**: Decision 3 (DashMap), AS-001, AS-004

**Problem**: `table_names()`, `snapshot_ticks()`, and `latest_snapshot()` all iterate DashMaps without any snapshot guarantee. A concurrent create/drop during iteration can produce:

- `table_names()` returns a name for a table that no longer exists (or omits one that was being created).
- `write_checkpoint` uses the snapshot returned by `latest_snapshot()`, then iterates its `table_names()` — but the snapshot's table list may be stale w.r.t. the actual DashMap state.
- The filesystem I/O in `write_checkpoint` happens outside any lock — a concurrent `drop_table` between the snapshot read and the IPC write produces a file for a table already removed from the store.

**Impact**: Transient inconsistencies in inspector output and checkpoint data.

**Recommended Correction**: Document that these methods return best-effort snapshots. For `write_checkpoint`, note that the snapshot itself is taken atomically (single `Arc<WorldSnapshot>`), so the snaphshot's table list is internally consistent. The DashMap metadata methods (`table_names()`, `snapshot_ticks()`) are only used by the inspector, not by any correctness-critical code path.