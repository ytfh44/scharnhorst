## Requirements

### Requirement: Versioned Columnar Storage
The system SHALL store Authority world state in a set of versioned Arrow tables, allowing multiple snapshots to coexist. Non-Authority (Derived, Ephemeral) state SHALL live outside `ArrowStore` in subsystem-owned stores (see `state-tiering` spec).

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
The ArrowStore SHALL expose an immutable Authority `WorldSnapshot` reference for read-only consumers. All reads flow through the `query-engine` -- which acts as the unified read interface for Authority state -- so no consumer may hold a direct reference to Arrow tables or mutate any data. All write operations MUST go through the `journal-system`.

#### Read path
`bevy-bridge` / `rule-ir` evaluator / `sim-scheduler` systems -> `query-engine` -> Authority `WorldSnapshot` -> `ArrowStore`

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

### Requirement: Mutation Capability Boundary
The `arrow-store` capability SHALL expose world mutation operations only through initialization-owned or journal-owned capability handles. Simulation consumers MUST NOT receive a handle that can call `create_table`, `drop_table`, `apply_diffs`, checkpoint ingestion, type registration, mutation-mode changes, or direct batch replacement.

The default simulation-facing store surface SHALL be read-only. If a forbidden mutation path is reached at runtime due to stale handles or integration mistakes, the method SHALL return an `ArrowStoreError` or mapped domain error rather than panic.

#### Scenario: Simulation code cannot apply diffs
- **WHEN** a simulation system is constructed for tick execution
- **THEN** it receives query and journal submission capabilities
- **AND** it does not receive an `ArrowStore` mutation capability capable of calling `apply_diffs`

#### Scenario: Stale initialization handle rejected
- **WHEN** an initialization mutation handle is used after the world enters Simulation phase
- **THEN** the operation returns a descriptive error
- **AND** no table, snapshot, index, or type registry mutation is applied

### Requirement: Authority-Only Arrow Storage
During simulation, `ArrowStore` SHALL contain only Authority state as defined by the `state-tiering` capability. Derived and Ephemeral state MUST NOT be inserted into authority tables, persisted snapshots, checkpoint files, or replay hash inputs.

Authority table creation SHALL require tier metadata or a table specification that implies Authority classification. Non-authority state must live in subsystem-owned Derived or Ephemeral storage outside `ArrowStore`.

#### Scenario: Creating an authority table during initialization
- **WHEN** content compilation creates the `actor_state` table before schema freeze
- **THEN** the table is registered as Authority state
- **AND** it is eligible for journal diffs, snapshots, save files, and deterministic hashes

#### Scenario: Rejecting ephemeral table creation
- **WHEN** runtime code attempts to create a `hovered_province` table in `ArrowStore`
- **THEN** the system rejects the operation because hover state is Ephemeral
- **AND** the caller must store that state in the Bevy/UI layer

### Requirement: Initialization-Only Structural Mutation
Structural mutations to the Arrow store SHALL be limited to the Initialization phase unless they are part of a future schema-migration capability explicitly designed for runtime use.

Structural mutations include creating tables, dropping tables, registering storage types, changing mutation modes, loading checkpoint table batches, or replacing whole tables outside journal replay.

#### Scenario: Creating tables before simulation
- **WHEN** the content-loader runs during cold-start content compilation
- **THEN** it may use an initialization capability to create Authority tables
- **AND** the capability becomes unavailable before the scheduler starts the first tick

#### Scenario: Creating tables after simulation starts
- **WHEN** a system attempts to create a table during the Economy phase
- **THEN** the request is rejected with an error
- **AND** the schema registry and Arrow store remain unchanged

### Requirement: Journal-Owned Diff Application
`ArrowStore` SHALL apply simulation diffs only when called through a journal-owned `CommitStore` capability handle. No other crate or subsystem may apply committed or uncommitted simulation diffs directly to Authority tables.

The `CommitStore` handle SHALL be constructible from `Arc<ArrowStore>` by framework crates (arrow-store, journal, scheduler). External simulation consumers SHALL NOT receive an `Arc<ArrowStore>` handle, preventing `CommitStore` construction at the simulation boundary.

#### Scenario: Journal commits authority diffs
- **WHEN** the scheduler reaches the end of `PostTick`
- **THEN** `journal-system` uses its `CommitStore` handle to apply the tick's diffs to `ArrowStore`
- **AND** the store generates the committed snapshot for that tick

#### Scenario: Direct diff application rejected
- **WHEN** a debug tool attempts to call the store diff application path directly
- **THEN** the call is unavailable or rejected
- **AND** the tool must submit a debug write through `DebugWriteJournal`

### Requirement: Patch Operations Preserve Null Semantics
Diff application patch operations (`patch_primitive_array`, `patch_boolean_array`, `patch_string_array`) SHALL preserve the null bitmap from source RecordBatches. When patching individual row values, null entries in unaffected rows MUST remain null in the output.

`patch_string_array` SHALL return the appropriate `GenericStringArray` variant matching the input offset type: `GenericStringArray<i32>` for `DataType::Utf8` and `GenericStringArray<i64>` for `DataType::LargeUtf8`. The return type MUST NOT be hardcoded.

#### Scenario: Patching a row in a table with null entries
- **WHEN** a diff updates one row in a table where other rows contain null values in the target column
- **THEN** the patch operation produces an output array where null values in unaffected rows remain null
- **AND** only the target row's value is modified

#### Scenario: Patching a LargeUtf8 column
- **WHEN** a diff updates a string value in a `LargeUtf8` column
- **THEN** `patch_string_array` returns `GenericStringArray<i64>` matching the input type
- **AND** the output array is compatible with the original RecordBatch schema

### Requirement: Duplicate RowId Detection
`apply_diff_insert` SHALL check for duplicate `RowId` before inserting into the position map. If a `RowId` already exists in the table's `RowPositionMap`, the insertion SHALL return `ArrowStoreError::DuplicateRowId` rather than silently overwriting the existing entry. Silent overwrite causes the old row's Arrow batch data to become orphaned — present in the batch but unreachable via the position map.

#### Scenario: Duplicate RowId rejected
- **WHEN** a `Diff::Insert` for table `actor_state` references a RowId already present in the position map
- **THEN** the insert returns `ArrowStoreError::DuplicateRowId`
- **AND** neither the position map nor the Arrow batch data is modified for the duplicate row

### Requirement: Checkpoint Loading Fully Initializes Tables
When `load_checkpoint` creates tables during checkpoint restoration, it SHALL register each created table with a `TableSpec`, a `TableId` in `table_id_map`, and a `MutationMode`. Tables created by `load_checkpoint` SHALL be indistinguishable from tables created by `create_table` during initialization, except that the `MutationMode` is currently always `AppendOnly` for checkpoint-loaded tables because the checkpoint format does not yet record the mode per table. The restore path SHALL acquire `create_drop_lock` before inserting into `tables` and `table_id_map`, matching the serialization protocol used by `create_table`.

#### Scenario: Loading checkpoint restores complete table state
- **WHEN** a checkpoint is loaded containing authority tables
- **THEN** each table has a registered `TableSpec`, `TableId`, and `MutationMode`
- **AND** `drop_table` can locate the table by `TableId` through `table_id_map`
- **AND** `build_primary_key_index` can access the table's `TableSpec`

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

### I-AS-JOURNAL-OWNED
`CommitStore` is the sole handle capable of calling `apply_diffs` on Authority tables. `CommitStore::new()` requires `Arc<ArrowStore>` — external simulation consumers SHALL NOT receive `Arc<ArrowStore>`, preventing `CommitStore` construction at the simulation boundary. Systems that need to submit diffs do so through `Journal::submit_diff()` which routes through `CommitStore` at the scheduler's tick-boundary commit. No simulation system can apply diffs directly.

### I-AS-AUTHORITY-ONLY
All tables in `ArrowStore` are Authority state. `create_table` during Initialization requires tier classification implying Authority. Derived state caches, Ephemeral UI state, and transient temporal state live outside `ArrowStore`. Save snapshots and deterministic state hashes exclude non-Authority tables by construction.

### I-AS-INIT-STRUCTURAL
Structural mutations (create_table, drop_table, type registration, mutation-mode changes, checkpoint loading) are legal only during Initialization. After the simulation phase begins, structural mutations require a schema-migration capability that does not yet exist. A stale initialization handle used during Simulation returns a descriptive error.

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

