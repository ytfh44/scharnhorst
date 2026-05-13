# Arrow Store — Lock-Free Optimization Delta

## Overview

This spec describes the behavioral implications of Phase 3 (ArrowStore lock sharding) and its interaction with existing requirements. The change replaces the single `Arc<RwLock<ArrowStoreInner>>` with per-field locking using `DashMap` for collections and `AtomicU64` for counters.

**Baseline**: [openspec/specs/arrow-store/spec.md](openspec/specs/arrow-store/spec.md)

---

## AS-001: Table Creation and Lookup

### Baseline Requirement

Tables are created via `create_table(spec, mode)`, assigned a monotonically increasing `TableId`, and stored keyed by name. Duplicate names are rejected. Lookup is by name via `get_table`, which returns a clone.

### Behavioral Impact

**ID allocation**: `next_table_id` changes from `u64` field on `ArrowStoreInner` to `AtomicU64`. The existing code at [store.rs:L280-L281](scharnhorst_arrow_store/src/store.rs#L280-L281):
```rust
let id = TableId::new(inner.next_table_id);
inner.next_table_id += 1;
```
becomes:
```rust
let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
let id = TableId::new(id_val);
```
`fetch_add` returns the *old* value, which is the correct new ID. The monotonicity guarantee is preserved — `AtomicU64::fetch_add` has stronger monotonicity than the non-atomic `inner.next_table_id += 1` (which was only safe because the write lock serialized all writers).

**Serialization**: All `create_table` and `drop_table` calls are globally serialized by `create_drop_lock: Mutex<()>`. Unlike the old Big Lock (which blocked ALL ArrowStore operations), this lock only blocks other `create_table`/`drop_table` calls — reads and writes on unrelated tables proceed concurrently. The old code used the global RwLock's write guard for the same serialization, so create/drop concurrency semantics are unchanged: at most one of them executes at any instant. Duplicate-name rejection is provided by an explicit `contains_key` check inside the lock, not by DashMap's per-shard locking.

**ID allocation and `create_drop_lock` serialization**: `next_table_id.fetch_add(1, Relaxed)` allocates a monotonically unique `TableId`. The two DashMap inserts (`tables`, then `table_id_map`) are serialized by `create_drop_lock: Mutex<()>`, making them atomically observable to any caller that also acquires `create_drop_lock` (i.e., concurrent `create_table`/`drop_table`). The `fetch_add` is deliberately placed BEFORE `create_drop_lock` acquisition: this avoids holding the lock across an atomic operation and accepts that an ID may be skipped if `create_table` subsequently fails (duplicate name). Skipped IDs are harmless — `TableId` values only need to be unique, not contiguous.

**Partial state visibility**: Since `create_drop_lock` serializes the insert pair, no concurrent `create_table` or `drop_table` can observe the intermediate state where a table exists in `tables` but not `table_id_map`. Readers that do NOT acquire `create_drop_lock` (e.g., `table_names()`, `get_table()`) may observe transient states during concurrent create/drop, but those states are self-consistent: a name appearing in `table_names()` while absent from `table_id_map` simply means the reverse mapping hasn't been written yet — the next `drop_table` of that name will find it via `table_id_map` iteration.

**Evidence**: The new `create_table` at [store.rs:L255-L277](scharnhorst_arrow_store/src/store.rs#L255-L277) first checks for duplicate names (racy check outside lock), then allocates an ID, then acquires `create_drop_lock`, re-checks for duplicates, and performs both inserts. The racy duplicate-check outside the lock is a fast-path optimization; the authoritative check inside the lock guarantees correctness.

---

## AS-002: Table Deletion

### Baseline Requirement

`drop_table(name)` removes the table and any associated ID mapping, returning `TableNotFound` if the name does not exist.

### Behavioral Impact

**Removal order under `create_drop_lock`**: Both `create_table` and `drop_table` now acquire `create_drop_lock: Mutex<()>`, serializing their two-step DashMap operations into a single atomic critical section (per Amendment AS-CA-001). Within this critical section, `drop_table` iterates `table_id_map` to locate the `TableId` matching the given table name, removes the `table_id_map` entry, then removes the table from `tables`. Since both steps are serialized by the `create_drop_lock`, the removal order is unobservable by any concurrent caller — the table and its ID mapping disappear atomically.

**Orphan in table_id_map**: Under the `create_drop_lock`, the window between `table_id_map.remove()` and `tables.remove()` is invisible to any other thread (they block on `create_drop_lock`). A crash between the two removes would produce a stale `table_id_map` entry without a corresponding table, but no code iterates `table_id_map` independently — it is only used during `drop_table` for the name→ID reverse lookup.

**Evidence**: `drop_table` at [store.rs:L279-L293](scharnhorst_arrow_store/src/store.rs#L279-L293) acquires `create_drop_lock`, iterates `table_id_map` to find the matching `TableId`, removes the `table_id_map` entry, then removes the table from `tables`. The old code at [store.rs:L295-L310](scharnhorst_arrow_store/src/store.rs#L295-L310) required `inner.table_id_map.iter().find(|(_, n)| *n == name)` because there was no efficient TableId→name reverse mapping — this pattern is retained under the new serialization lock.

---

## AS-003: Snapshot Generation

### Baseline Requirement

`generate_snapshot(tick)` produces a `WorldSnapshot` containing all tables at their current state, stored keyed by tick. Returns `Arc<WorldSnapshot>`.

### Behavioral Impact — Re-entrancy Constraint Eliminated

**Critical behavioral change**: The old code at [store.rs:L767-L768](scharnhorst_arrow_store/src/store.rs#L767-L768) contains an explicit re-entrancy workaround:
```rust
// Inline snapshot generation -- cannot call generate_snapshot
// while holding the write lock (RwLock is not reentrant).
```
This forced `load_checkpoint` to duplicate ~15 lines of snapshot generation logic inline. With per-table locking, `generate_snapshot` no longer acquires the outer write lock on `ArrowStoreInner` — it acquires per-table read locks via `Arc<RwLock<VersionedTable>>`. `load_checkpoint` can now call `self.generate_snapshot(tick)` directly, eliminating code duplication and the re-entrancy constraint.

**Evidence**: The old `load_checkpoint` at [store.rs:L754-L783](scharnhorst_arrow_store/src/store.rs#L754-L783) held the outer write lock (line 754: `let mut inner = self.write("load_checkpoint")?;`) and inlined snapshot generation (lines 769-782). Post-change, these two lock acquisitions do not conflict.

**Snapshot iteration consistency**: The old `generate_snapshot` iterated `inner.tables` under a global write lock, guaranteeing a frozen view of all tables. With DashMap iteration, the view is no longer frozen — per-table read locks are acquired sequentially during iteration. In principle a concurrent `apply_diffs` on a table not yet visited could modify that table before the snapshot reaches it, producing a snapshot where table A reflects time T1 and table B reflects time T2 (T2 > T1). `create_table`/`drop_table` are globally serialized by `create_drop_lock`, so they cannot introduce non-deterministic entry addition/removal mid-iteration (the iterator sees whichever set of entries exists when it reaches each shard; this is no less deterministic than any competing-critical-section pattern).

**Mitigation**: In practice, `generate_snapshot` is only called from `journal.commit()`, which runs in `atomic_commit()` at [scheduler.rs:L252-L258](scharnhorst_scheduler/src/scheduler.rs#L252-L258). During commit, no other systems run (sequential phase design at [scheduler.rs:L210-L218](scharnhorst_scheduler/src/scheduler.rs#L210-L218)), so there is never a concurrent writer during snapshot generation. The per-table locking primarily benefits non-tick-boundary reads (inspector queries, AI evaluation). The documented constraint on `generate_snapshot` (must not be called concurrently with mutations when cross-table consistency is required — per AS-CA-002) covers the remaining risk.

**Snapshot deduplication invariant**: Old code: `inner.generation += 1` at [store.rs:L451](scharnhorst_arrow_store/src/store.rs#L451). New code: `self.generation.fetch_add(1, Ordering::Relaxed)`. Both are strictly monotonic; the AtomicU64 version additionally guarantees no lost updates under concurrent snapshot generation (which, as noted, does not occur in practice).

---

## AS-004: Snapshot Retrieval

### Baseline Requirement

`get_snapshot(tick)`, `latest_snapshot()`, `snapshot_ticks()` retrieve snapshots stored by previous `generate_snapshot` calls.

### Behavioral Impact

**Concurrent read availability**: Under the old Big Lock, a write-locked `generate_snapshot` blocked ALL `get_snapshot` calls. With DashMap `snapshots`, the new snapshot is inserted atomically into the concurrent map, and `get_snapshot` calls on other ticks proceed without blocking. This is the primary concurrency win — snapshot reads and writes are now independent.

**Evidence**: The old `get_snapshot` at [store.rs:L473-L480](scharnhorst_arrow_store/src/store.rs#L473-L480) acquired `self.read("get_snapshot")?`, which was blocked by any writer. The new code acquires a read-lock on only the specific DashMap shard, which is independent of the shard being written.

**Latest snapshot ordering**: `latest_snapshot()` at [store.rs:L482-L491](scharnhorst_arrow_store/src/store.rs#L482-L491) iterates all snapshot keys to find `max()`. Under DashMap, a concurrent `generate_snapshot(T_new)` may insert a key that is > the current max while iteration is in progress. Since `generate_snapshot` runs inside the sequential `atomic_commit()` phase (not concurrent), this is not an issue in practice. The worst case is that `latest_snapshot()` returns the N-1th snapshot instead of the Nth, which is indistinguishable from the caller racing with the commit thread.

---

## AS-005: Mutation Modes

### Behavioral Impact

**None.** `set_mutation_mode` at [store.rs:L337-L345](scharnhorst_arrow_store/src/store.rs#L337-L345) now acquires a write lock on a specific `Arc<RwLock<VersionedTable>>` instead of the global write lock. The per-table lock prevents concurrent modifications to the same table while allowing modifications to different tables.

**Evidence**: `mutation_mode()` at [store.rs:L347-L354](scharnhorst_arrow_store/src/store.rs#L347-L354) previously acquired a global read lock. Now it acquires a per-table read lock. The observable behavior is identical — the caller sees the current mutation mode — but the latency is lower when other tables are being written.

---

## AS-006: IPC and Checkpointing

### Baseline Requirement

`load_checkpoint(tick, table_to_batches)` loads state from a checkpoint, creating tables and inserting data, then producing a snapshot.

### Behavioral Impact

**Code deduplication**: The old code at [store.rs:L754-L783](scharnhorst_arrow_store/src/store.rs#L754-L783) wrote the entire checkpoint under a single write lock: create tables, insert data, inline generate snapshot. With per-table locking, the checkpoint load can call `self.generate_snapshot(tick)` instead of inlining. This eliminates the bug-prone code duplication noted at [store.rs:L767-L768](scharnhorst_arrow_store/src/store.rs#L767-L768).

**Progressive visibility**: Under the old Big Lock, checkpoint loading was fully opaque to concurrent readers — they saw either the complete pre-load state or nothing changed. With per-table locking, a concurrent `table_names()` could observe some tables already created while others are still being inserted. However, since checkpoint loading happens at application startup (no concurrent traffic), this is not a practical concern.

**Evidence**: `load_checkpoint` at [store.rs:L753-L754](scharnhorst_arrow_store/src/store.rs#L753-L754) previously required "Acquire write lock, create tables if needed, insert data, and generate snapshot all in one critical section." Post-change, the write lock on inner is removed; table creation and snapshot generation use their respective per-field locks.

---

## AS-007: Type Registry

### Baseline Requirement

Custom storage types can be registered via `register_type` and resolved via `resolve_type`. The default registry includes seven built-in types.

### Behavioral Impact

**Isolated locking**: The `type_registry` field at [store.rs:L216](scharnhorst_arrow_store/src/store.rs#L216) was previously protected by the global RwLock. Now it has its own `RwLock<TypeRegistry>`. Since `TypeRegistry` is `Clone` (at [store.rs:L62](scharnhorst_arrow_store/src/store.rs#L62)) and composed of `Arc` fields, registration and resolution operate on a standalone hashmap with minimal contention. The `default_type_registry()` function at [store.rs:L119-L197](scharnhorst_arrow_store/src/store.rs#L119-L197) creates a new registry with 7 built-in types once at store initialization; post init, reads vastly outnumber writes (types are registered at startup, rarely during runtime).

**Evidence**: `TypeRegistry::register` at [store.rs:L81-L96](scharnhorst_arrow_store/src/store.rs#L81-L96) takes `&mut self`, so it requires a write lock regardless. The isolated `RwLock` means type lookups via `resolve_data_type` at [store.rs:L109-L116](scharnhorst_arrow_store/src/store.rs#L109-L116) are no longer serialized with table operations.

---

## AS-008: Diff Application

### Baseline Requirement

`apply_diffs(tick, diffs)` applies a list of `Diff` operations to tables. Each diff targets a specific table/row/column.

### Behavioral Impact — Per-Table Atomic Diff Application

**Design goal**: Each diff operation must be applied atomically to its target table. For a single `Diff`, all reads (position_map, versions, batches) and the final write (patch, delete, insert) must occur under a single held write lock. This prevents interleaving with concurrent mutations to the same table.

**Restructured approach** (per Amendment AS-CA-004): `ArrowStore::apply_diffs()` dispatches each diff by acquiring the per-table write lock **once**, then delegates to inner functions that accept `&mut VersionedTable` + `&TypeRegistry`:

```rust
pub fn apply_diffs(&self, tick: Tick, diffs: &[Diff]) -> ArrowStoreResult<()> {
    for diff in diffs {
        let table_name = diff.table_name();
        if let Some(entry) = self.tables.get_mut(table_name) {
            let mut table = entry.write()?;
            apply_diff_to_table(tick, diff, &mut table, &self.type_registry)?;
        }
    }
    Ok(())
}
```

The inner functions (`apply_update`, `apply_delete`, `apply_insert`, `apply_replace`) operate on the already-locked `&mut VersionedTable` and perform all reads-then-writes within that single critical section.

**Pre-Amendment behavior (replaced)**: The old code dispatched to `ArrowStoreInner::apply_*` methods that performed multiple independent `self.tables.get(…)` → `RwLock::read()/write()` calls per diff. Between each call, the lock on the target table was released, allowing interleaving with concurrent mutations to the same table. Amendment AS-CA-004 identified this as a race condition and prescribed the current one-lock-per-diff pattern.

**Concurrency win**: Diffs targeting **different** tables acquire separate per-table write locks and can proceed concurrently. Diffs targeting the **same** table are serialized by the single `Arc<RwLock<VersionedTable>>` — but critically, each individual diff sees a consistent snapshot of that table for the duration of its read-modify-write cycle.

**Error propagation preserved**: Each inner function returns `ArrowStoreResult<()>`. If any diff fails, `apply_diffs` returns immediately with the error, and subsequent diffs in the list are not applied. This matches the pre-existing contract where the global write lock prevented partial application.

**Evidence**: The diff dispatch in `ArrowStore::apply_diffs()` matches on `Diff::Update`, `Diff::Insert`, `Diff::Delete`, `Diff::ReplaceTable`. Each match arm acquires `self.tables.get_mut(table_name)` (a single write-lock acquisition), then calls the corresponding inner function that consumes `&mut VersionedTable` and `&TypeRegistry` without further lock acquisitions.

---

## New Invariants Introduced

1. **I-AS-INTRA-TICK**: During `Scheduler::atomic_commit()`, all mutations to `ArrowStore` are single-threaded (sequential phase design). Therefore, DashMap's non-snapshot-consistent iteration semantics are irrelevant within the tick boundary.

2. **I-AS-INTER-TABLE**: Operations on different tables never block each other. The only blocking occurs when two operations target the same table's `Arc<RwLock<VersionedTable>>`.

3. **I-AS-ORPHAN**: `table_id_map` may transiently contain entries for tables absent from `tables` millisecond-scale windows. No code path iterates `table_id_map` independently (only via `find()` during drop), so these orphans are unobservable.

4. **I-AS-GENERATION**: `generation` is `AtomicU64` and incremented via `fetch_add` at the start of `generate_snapshot()` — before the snapshot assembly logic. Consequently the counter reflects snapshot *attempts* rather than successful snapshots: a failed `generate_snapshot` (e.g. per-table lock acquisition error) still increment the counter. This is harmless because the failed call returns `Err` to its caller and no snapshot is stored in `self.snapshots`; the skipped generation value has no observable effect other than a gap in the monotonic sequence. Readers see strictly non-decreasing values.

Note: This `generation` is independent of the Scheduler's own `generation` counter (see [sim-scheduler spec](../sim-scheduler/spec.md) I-SCHED-TICK-ATOMIC). The Scheduler increments *its* `generation` in `atomic_commit()` after the journal commit; the ArrowStore increments *its* `generation` at the start of `generate_snapshot()` which is called *by* the journal commit. The two counters have no happens-before relationship with each other and serve separate purposes: the Scheduler's generation tracks commit cycles, while the ArrowStore's generation tracks snapshot generation attempts.

## Invariants Preserved

- All public API return types and error variants preserved.
- `serde`-serialized state format unchanged (store.rs external interface unchanged).
- `ArrowStoreError` variants unchanged (new error paths map to same variants).
- `VersionedTable` fields remain publicly mutable and externally synchronized per [versioned_table.rs:L40-L71](scharnhorst_arrow_store/src/versioned_table.rs#L40-L71) — the Arc<RwLock<>> wrapping provides this synchronization.

---

## Critical Analysis Amendments

This section documents issues discovered during deep review of the ArrowStore sharding design. Each amendment includes the problem, impact, and recommended correction.

---

### AS-CA-001: `create_table`/`drop_table` Race — Orphan in `table_id_map` (CRITICAL)

**Location**: AS-001 (Table Creation and Lookup), AS-002 (Table Deletion)

**Problem**: `create_table` does two sequential DashMap inserts — first into `tables`, then into `table_id_map`. The new `drop_table` does two sequential DashMap removes — first from `tables`, then from `table_id_map`. A concurrent interleaving:

| Step | create_table thread          | drop_table thread              |
|------|------------------------------|--------------------------------|
| 1    | `tables.insert("X", ...)`    |                                |
| 2    |                              | `tables.remove("X")` — found  |
| 3    |                              | `table_id_map.remove(id)` — no entry yet, silently skipped |
| 4    | `table_id_map.insert(id, "X")` |                              |

produces an orphan entry in `table_id_map` with no corresponding table.

**Mitigation in design**: The design says "no code path iterates table_id_map independently." Under the new code, `drop_table` no longer iterates `table_id_map` (it uses the `TableId` returned from `tables.remove()`). The orphan is unobservable but accumulates garbage.

**Recommended Correction**: Introduce a `create_drop_lock: Arc<Mutex<()>>` on `ArrowStore` (`Arc` is needed because `ArrowStore` derives `Clone` for sharing across threads). This serializes the two inserts (or two removes) atomically:

```rust
let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
let id = TableId::new(id_val);
let _lock = self.create_drop_lock.lock().map_err(...)?;
self.tables.insert(spec.name.clone(), ...);
self.table_id_map.insert(id, spec.name.clone());
drop(_lock);
```

Contention is negligible (create/drop are rare).

---

### AS-CA-002: `generate_snapshot` Is Not Point-in-Time (SIGNIFICANT)

**Location**: AS-003 (Snapshot Generation)

**Problem**: The old Big Lock guaranteed all tables in a snapshot reflect a single frozen point in time. With per-table DashMap iteration, table A is snapshotted at T1 and table B at T2 (T2 > T1). A concurrent mutation to table B between T1 and T2 means the snapshot contains table A at T1 and table B at T2 — potentially violating cross-table foreign-key consistency.

The design's sequential-phase mitigation covers only the scheduler's commit path. The spec explicitly states "per-table locking primarily benefits non-tick-boundary reads" — but if those reads (inspector, AI evaluation) call `generate_snapshot` concurrently with mutations, the snapshot is NOT point-in-time.

**Recommended Correction**: Document on `generate_snapshot` that it must not be called concurrently with mutating operations when cross-table consistency is required. Optionally: add a `snapshot_generation_lock` if concurrent snapshot generation becomes necessary.

---

### AS-CA-003: `truncate_before` — Snapshot/Version Interleaving (SIGNIFICANT)

**Location**: AS-004 (Snapshot Retrieval)

**Problem**: `truncate_before` does two steps: (1) retain snapshots with `t >= threshold`, (2) iterate tables and retain versions with `t >= threshold`. A concurrent `generate_snapshot(T_new)` between steps 1 and 2 may produce a snapshot whose `table_batches` reference versions that were removed in step 2.

**Recommended Correction**: Document that `truncate_before` must not run concurrently with `generate_snapshot`. In practice, both are called from the sequential commit path, but this should be explicitly stated.

---

### AS-CA-004: `apply_diffs` Multi-Step Read-Then-Write Must Be Atomic (MODERATE — APPLIED)

**Location**: AS-008 (Diff Application)

**Problem** (pre-fix): `apply_update` and sibling methods performed multiple independent `self.tables.get(table_name)` → `RwLock.read()/write()` calls per diff operation. Between each call, the lock on the target table was released, creating a window for concurrent mutations to interleave.

**Applied correction**: The diff application is restructured so that `ArrowStore::apply_diffs()` acquires the per-table write lock **once per diff** and holds it across the entire read-modify-write sequence. Inner functions (`apply_update`, `apply_delete`, `apply_insert`, `apply_replace`) accept `&mut VersionedTable` + `&TypeRegistry`, performing all operations under the single held lock. See AS-008 for the full design.

---

### AS-CA-005: DashMap Iteration Non-Determinism (MODERATE)

**Location**: AS-001 (Table Names), AS-004 (Snapshot Ticks)

**Problem**: `table_names()`, `snapshot_ticks()`, and `latest_snapshot()` iterate DashMaps without snapshot guarantees. Concurrent mutations during iteration may produce transiently inconsistent results (e.g., a name for a deleted table, or a missing name for a newly created table). `write_checkpoint` uses snapshot data, then iterates — the filesystem I/O happens outside any lock.

**Recommended Correction**: Document that these methods return best-effort snapshots. Only used by inspector — not correctness-critical.