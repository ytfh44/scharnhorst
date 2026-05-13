## Requirements

### Requirement: Versioned Columnar Storage
The system SHALL store world state in a set of versioned Arrow tables, allowing multiple snapshots to coexist.

#### Scenario: Accessing a specific generation
- **WHEN** requesting a snapshot of the world at generation 500
- **THEN** the store returns the table batches corresponding to that specific generation.

### Requirement: Partitioned Table Access
The system MUST support partitioning tables (e.g., by region) to enable parallel scanning and partial updates.

#### Scenario: Regional scan
- **WHEN** scanning for population groups in "region_north"
- **THEN** the system only reads the partitions associated with that region.

### Requirement: Mutation Modes
The system SHALL support different mutation modes (`AppendOnly`, `Patchable`, `RebuildPerTick`) to optimize write performance based on table usage.

#### Scenario: Appending to event log
- **WHEN** a new event is emitted
- **THEN** the system uses `AppendOnly` mode to add a new batch to the "event_log" table without modifying existing data.

### Requirement: RowPositionMap (RowId -> Physical Position)
The system MUST produce and maintain a `RowPositionMap` for each table that translates logical `RowId` values to physical `(batch_index, row_offset)` tuples.

#### Position map semantics:
1. On insert: add entry `RowId -> (batch_idx, offset)`
2. On delete: remove the entry for the deleted RowId (DO NOT re-index other entries)
3. On batch compaction (`RebuildPerTick` or `ReplaceTable`): rebuild all position entries to reflect new batch layout. RowIds SHALL be assigned **sequentially from 0** using a **global counter across all batches** (not per-batch). This ensures each row has a unique RowId and the position map correctly maps each RowId to its `(batch_idx, offset)`.
4. The authoritative `RowPositionMap` is stored on `VersionedTable` (accessible via `WorldSnapshot::get_table(table_name)?.position_map()`). During snapshot ingestion, a read-only copy is stored on `TableReadView` within the `query-engine`, ensuring fast RowId->position resolution without locking the ArrowStore.
5. `RowId.0` MUST NOT be used as a physical array index -- always resolve through the position map

#### Scenario: Evaluator reads a specific row
- **WHEN** the rule-ir evaluator requests `lookup_row("actor_state", RowId(7))`
- **THEN** the system consults `RowPositionMap` -> finds `(batch_idx=2, offset=3)` -> reads the value from `batches[2]` at row `3`
- **AND** does NOT attempt `batches[0].column(col)[7]` (which would be the wrong row)

### Requirement: Supported Arrow Data Types
The system SHALL support the following storage-to-Arrow type mappings:

| Storage type name | Arrow DataType | Notes |
|-------------------|----------------|-------|
| `i64` | `DataType::Int64` | Signed 64-bit integer |
| `i32` | `DataType::Int32` | Signed 32-bit integer |
| `u64` | `DataType::UInt64` | Unsigned 64-bit integer |
| `f64` | `DataType::Float64` | Double-precision float |
| `utf8` | `DataType::Utf8` | Variable-length UTF-8 string |
| `large_utf8` | `DataType::LargeUtf8` | Large variable-length string |
| `bool` | `DataType::Boolean` | Boolean |

The set SHALL be extensible via the schema-registry via **instance-scoped registration**. Each `ArrowStore` SHALL maintain its own `TypeRegistry`. The `register_extra_type()` free function is replaced with `ArrowStore::register_type()`. Adding a new storage type requires registering its Arrow `DataType`, JSON deserializer, and IPC roundtrip support.

### Requirement: Delete Semantics
When a row is deleted via `Diff::Delete`:

1. The `RowPositionMap` entry for the deleted `RowId` SHALL be removed, making the row invisible to all subsequent `query-engine` operations (`lookup_row` returns `RowNotFound`).
2. For the underlying Arrow batch data:
   - Columns declared `nullable=true` in the `ColumnSpec` SHALL be set to null values.
   - Columns declared `nullable=false` SHALL be preserved with their original values. The `RowPositionMap` removal is the authoritative deletion signal; the underlying data exists only for batch structural consistency.
3. The `build_null_patch` helper SHALL respect the source batch schema's nullability metadata when constructing the output batch.
4. This ensures: after DELETE, `lookup_row(table, deleted_row_id)` returns `RowNotFound`, regardless of column nullability.

#### Scenario: Deleting a non-nullable column row
- **WHEN** a row with `id: i64 NOT NULL` is deleted
- **THEN** the `RowPositionMap` entry is removed, making the row invisible
- **AND** the `id` column in the underlying batch retains its original value (not null)
- **AND** `query-engine.lookup_row(table, deleted_row_id)` returns `RowNotFound`
- **AND** attempting to read the deleted row's position via `RowPositionMap` fails

#### Scenario: Deleting a nullable column row
- **WHEN** a row with nullable columns (e.g., `description: utf8 NULL`) is deleted
- **THEN** nullable columns SHALL be set to null in the underlying batch
- **AND** non-nullable columns SHALL retain their original values

### Requirement: Checkpoint & Diff Replay (save-system, journal-system)
The system SHALL periodically generate full snapshots (checkpoints) and support replaying the `SaveJournal` diffs from the last checkpoint for state reconstruction.

#### Scenario: Auto-checkpoint on journal overflow
- **WHEN** the save journal accumulates more than 1000 tick diffs without a checkpoint
- **THEN** the store writes a new full `WorldSnapshot` to disk and truncates the journal from that point.

#### Scenario: State reconstruction from checkpoint
- **WHEN** loading a save game
- **THEN** the system loads the latest full snapshot and replays all subsequent diffs from the save journal.

### Requirement: Instance-Scoped Type Registry
The type registry SHALL be **instance-scoped**, not global. Each `ArrowStore` instance SHALL maintain its own `TypeRegistry` with the 7 built-in types pre-registered. This eliminates test pollution from shared global mutable state and avoids lock contention on read-heavy paths.

#### Architecture
1. `ArrowStore` SHALL own a `TypeRegistry` as a field within its inner state.
2. The free function `register_extra_type()` SHALL be removed. `ArrowStore::register_type()` SHALL be the sole registration API.
3. The global `static TYPE_REGISTRY` SHALL be removed.
4. Internal helpers (`storage_type_to_arrow`, `json_value_to_array`) SHALL receive a `&TypeRegistry` parameter instead of locking the global.

#### Rationale
- Tests registering extra types no longer interfere with each other
- No global lock contention on read paths
- Type registry scope matches ArrowStore lifecycle

### Requirement: Read-Only Access via Query Engine (query-engine)
The ArrowStore SHALL expose an immutable `WorldSnapshot` reference for read-only consumers. All reads flow through the `query-engine` -- which acts as the unified read interface -- so no consumer may hold a direct reference to Arrow tables or mutate any data. All write operations MUST go through the `journal-system`.

#### Read path
`bevy-bridge` / `rule-ir` evaluator / `sim-scheduler` systems -> `query-engine` -> `WorldSnapshot` -> `ArrowStore`

**Invariant**: No component outside the `query-engine` may access `ArrowStore` directly. This includes:
- Direct table scans (`snapshot.arrow_table().column()`) -- **forbidden**
- Direct partition access -- **forbidden**
- Direct RelationGraph queries -- **forbidden** (RelationGraph is owned by `schema-registry` and accessed via `query-engine`)

#### Scenario: Cross-partition scope jump
- **WHEN** rule-ir evaluates `jump_to(Relation::Owner)` on a province in "region_12"
- **THEN** the evaluator calls `query-engine.lookup_row("actor_state", actor_id)`
- **AND** the query-engine internally queries the `schema-registry`'s RelationGraph to resolve the target partition
- **AND** the query-engine performs an indexed lookup within that partition
- **THEN** the result is returned to the evaluator

---

## Invariants

### I-AS-INTRA-TICK
During `Scheduler::atomic_commit()`, all mutations to `ArrowStore` are single-threaded (sequential phase design). Therefore, DashMap's non-snapshot-consistent iteration semantics are irrelevant within the tick boundary.

### I-AS-INTER-TABLE
Operations on different tables never block each other. The only blocking occurs when two operations target the same table's `Arc<RwLock<VersionedTable>>`.

### I-AS-ORPHAN
`table_id_map` may transiently contain entries for tables absent from `tables` millisecond-scale windows. No code path iterates `table_id_map` independently (only via `find()` during drop), so these orphans are unobservable.

### I-AS-GENERATION
`generation` is `AtomicU64` and incremented via `fetch_add` at the start of `generate_snapshot()` — before the snapshot assembly logic. Consequently the counter reflects snapshot *attempts* rather than successful snapshots: a failed `generate_snapshot` (e.g. per-table lock acquisition error) still increment the counter. This is harmless because the failed call returns `Err` to its caller and no snapshot is stored in `self.snapshots`; the skipped generation value has no observable effect other than a gap in the monotonic sequence. Readers see strictly non-decreasing values.

Note: This `generation` is independent of the Scheduler's own `generation` counter (see sim-scheduler spec I-SCHED-TICK-ATOMIC). The Scheduler increments *its* `generation` in `atomic_commit()` after the journal commit; the ArrowStore increments *its* `generation` at the start of `generate_snapshot()` which is called *by* the journal commit. The two counters have no happens-before relationship with each other and serve separate purposes: the Scheduler's generation tracks commit cycles, while the ArrowStore's generation tracks snapshot generation attempts.

---

## Design Notes

### `create_table`/`drop_table` Race — Orphan in `table_id_map` (CRITICAL)

**Location**: Table Creation and Lookup, Table Deletion

**Problem**: `create_table` does two sequential DashMap inserts — first into `tables`, then into `table_id_map`. The `drop_table` does two sequential DashMap removes — first from `tables`, then from `table_id_map`. A concurrent interleaving:

| Step | create_table thread          | drop_table thread              |
|------|------------------------------|--------------------------------|
| 1    | `tables.insert("X", ...)`    |                                |
| 2    |                              | `tables.remove("X")` — found  |
| 3    |                              | `table_id_map.remove(id)` — no entry yet, silently skipped |
| 4    | `table_id_map.insert(id, "X")` |                              |

produces an orphan entry in `table_id_map` with no corresponding table.

**Mitigation in design**: No code path iterates `table_id_map` independently. `drop_table` uses the `TableId` returned from `tables.remove()`. The orphan is unobservable but accumulates garbage.

**Correction Applied**: Introduce a `create_drop_lock: Arc<Mutex<()>>` on `ArrowStore` (`Arc` is needed because `ArrowStore` derives `Clone` for sharing across threads). This serializes the two inserts (or two removes) atomically:

```rust
let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
let id = TableId::new(id_val);
let _lock = self.create_drop_lock.lock().map_err(...)?;
self.tables.insert(spec.name.clone(), ...);
self.table_id_map.insert(id, spec.name.clone());
drop(_lock);
```

Contention is negligible (create/drop are rare).

### `generate_snapshot` Is Not Point-in-Time (SIGNIFICANT)

**Location**: Snapshot Generation

**Problem**: The old Big Lock guaranteed all tables in a snapshot reflect a single frozen point in time. With per-table DashMap iteration, table A is snapshotted at T1 and table B at T2 (T2 > T1). A concurrent mutation to table B between T1 and T2 means the snapshot contains table A at T1 and table B at T2 — potentially violating cross-table foreign-key consistency.

The design's sequential-phase mitigation covers only the scheduler's commit path. The spec explicitly states "per-table locking primarily benefits non-tick-boundary reads" — but if those reads (inspector, AI evaluation) call `generate_snapshot` concurrently with mutations, the snapshot is NOT point-in-time.

**Correction Applied**: Documented on `generate_snapshot` that it must not be called concurrently with mutating operations when cross-table consistency is required. Optionally: add a `snapshot_generation_lock` if concurrent snapshot generation becomes necessary.

### `truncate_before` — Snapshot/Version Interleaving (SIGNIFICANT)

**Location**: Snapshot Retrieval

**Problem**: `truncate_before` does two steps: (1) retain snapshots with `t >= threshold`, (2) iterate tables and retain versions with `t >= threshold`. A concurrent `generate_snapshot(T_new)` between steps 1 and 2 may produce a snapshot whose `table_batches` reference versions that were removed in step 2.

**Correction Applied**: Documented that `truncate_before` must not run concurrently with `generate_snapshot`. In practice, both are called from the sequential commit path, but this is explicitly stated.

### `apply_diffs` Multi-Step Read-Then-Write Must Be Atomic (APPLIED)

**Location**: Diff Application

**Problem** (pre-fix): `apply_update` and sibling methods performed multiple independent `self.tables.get(table_name)` → `RwLock.read()/write()` calls per diff operation. Between each call, the lock on the target table was released, creating a window for concurrent mutations to interleave.

**Applied correction**: The diff application is restructured so that `ArrowStore::apply_diffs()` acquires the per-table write lock **once per diff** and holds it across the entire read-modify-write sequence. Inner functions (`apply_update`, `apply_delete`, `apply_insert`, `apply_replace`) accept `&mut VersionedTable` + `&TypeRegistry`, performing all operations under the single held lock.

### DashMap Iteration Non-Determinism (MODERATE)

**Location**: Table Names, Snapshot Ticks

**Problem**: `table_names()`, `snapshot_ticks()`, and `latest_snapshot()` iterate DashMaps without snapshot guarantees. Concurrent mutations during iteration may produce transiently inconsistent results (e.g., a name for a deleted table, or a missing name for a newly created table). `write_checkpoint` uses snapshot data, then iterates — the filesystem I/O happens outside any lock.

**Correction Applied**: Documented that these methods return best-effort snapshots. Only used by inspector — not correctness-critical.

---

## Implementation Notes

### Lock Sharding Architecture

The `ArrowStore` replaces the single `Arc<RwLock<ArrowStoreInner>>` with per-field locking:

```rust
pub struct ArrowStore {
    tables: DashMap<String, Arc<RwLock<VersionedTable>>>,
    table_id_map: DashMap<TableId, String>,
    snapshots: DashMap<Tick, Arc<WorldSnapshot>>,
    type_registry: Arc<RwLock<TypeRegistry>>,
    next_table_id: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    create_drop_lock: Arc<Mutex<()>>,  // serializes create/drop table pairs
}
```

### Table Creation and Deletion Serialization

Both `create_table` and `drop_table` acquire `create_drop_lock: Mutex<()>` to serialize their two-step DashMap operations:

```rust
pub fn create_table(&self, spec: TableSpec, mode: MutationMode) -> ArrowStoreResult<TableId> {
    let id_val = self.next_table_id.fetch_add(1, Ordering::Relaxed);
    let id = TableId::new(id_val);
    
    let _lock = self.create_drop_lock.lock().map_err(...)?;
    // Check for duplicate, then:
    self.tables.insert(spec.name.clone(), Arc::new(RwLock::new(table)));
    self.table_id_map.insert(id, spec.name.clone());
    Ok(id)
}
```

### ID Allocation

`next_table_id` uses `AtomicU64::fetch_add(1, Relaxed)`:
- Returns the old value as the new ID
- `fetch_add` guarantees monotonicity
- Skipped IDs are harmless if creation fails after ID allocation

### Snapshot Generation

The `generate_snapshot` function does not acquire the outer write lock:

```rust
pub fn generate_snapshot(&self, tick: Tick) -> ArrowStoreResult<Arc<WorldSnapshot>> {
    self.generation.fetch_add(1, Ordering::Relaxed);
    
    // Iterate tables, acquiring per-table read locks sequentially
    let mut table_snapshots = HashMap::new();
    for entry in self.tables.iter() {
        let table = entry.value().read()?;
        table_snapshots.insert(entry.key().clone(), table.snapshot());
    }
    
    let snapshot = Arc::new(WorldSnapshot::new(tick, table_snapshots));
    self.snapshots.insert(tick, snapshot.clone());
    Ok(snapshot)
}
```

### Diff Application

Each diff acquires the per-table write lock once:

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

### Re-entrancy Constraint Eliminated

With per-table locking, `generate_snapshot` does not acquire the outer write lock on `ArrowStoreInner`. `load_checkpoint` calls `self.generate_snapshot(tick)` directly, eliminating code duplication and the re-entrancy constraint.

