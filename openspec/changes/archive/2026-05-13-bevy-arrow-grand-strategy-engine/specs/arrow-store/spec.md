**Crate**: `scharnhorst_arrow_store`

## ADDED Requirements

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

### Requirement: RowPositionMap (RowId → Physical Position)
The system MUST produce and maintain a `RowPositionMap` for each table that translates logical `RowId` values to physical `(batch_index, row_offset)` tuples.

#### Position map semantics:
1. On insert: add entry `RowId → (batch_idx, offset)`
2. On delete: remove the entry for the deleted RowId (DO NOT re-index other entries)
3. On batch compaction (`RebuildPerTick` or `ReplaceTable`): rebuild all position entries to reflect new batch layout. RowIds SHALL be assigned **sequentially from 0** using a **global counter across all batches** (not per-batch). This ensures each row has a unique RowId and the position map correctly maps each RowId to its `(batch_idx, offset)`.
4. The authoritative `RowPositionMap` is stored on `VersionedTable` (accessible via `WorldSnapshot::get_table(table_name)?.position_map()`). During snapshot ingestion, a read-only copy is stored on `TableReadView` within the `query-engine`, ensuring fast RowId→position resolution without locking the ArrowStore.
5. `RowId.0` MUST NOT be used as a physical array index — always resolve through the position map

#### Scenario: Evaluator reads a specific row
- **WHEN** the rule-ir evaluator requests `lookup_row("actor_state", RowId(7))`
- **THEN** the system consults `RowPositionMap` → finds `(batch_idx=2, offset=3)` → reads the value from `batches[2]` at row `3`
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

### Requirement: Delete Semantics (CLARIFIED)
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

## MODIFIED Requirements

### (Compatibility) Requirement: Checkpoint & Diff Replay (→ save-system, ↔ journal-system)
The system SHALL periodically generate full snapshots (checkpoints) and support replaying the `SaveJournal` diffs from the last checkpoint for state reconstruction.

#### Scenario: Auto-checkpoint on journal overflow
- **WHEN** the save journal accumulates more than 1000 tick diffs without a checkpoint
- **THEN** the store writes a new full `WorldSnapshot` to disk and truncates the journal from that point.

#### Scenario: State reconstruction from checkpoint
- **WHEN** loading a save game
- **THEN** the system loads the latest full snapshot and replays all subsequent diffs from the save journal.

### Requirement: Instance-Scoped Type Registry (CLARIFIED)
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

### (Compatibility) Requirement: Read-Only Access via Query Engine (← query-engine)
The ArrowStore SHALL expose an immutable `WorldSnapshot` reference for read-only consumers. All reads flow through the `query-engine` — which acts as the unified read interface — so no consumer may hold a direct reference to Arrow tables or mutate any data. All write operations MUST go through the `journal-system`.

#### Read path
`bevy-bridge` / `rule-ir` evaluator / `sim-scheduler` systems → `query-engine` → `WorldSnapshot` → `ArrowStore`

**Invariant**: No component outside the `query-engine` may access `ArrowStore` directly. This includes:
- Direct table scans (`snapshot.arrow_table().column()`) — **forbidden**
- Direct partition access — **forbidden**
- Direct RelationGraph queries — **forbidden** (RelationGraph is owned by `schema-registry` and accessed via `query-engine`)

#### Scenario: Cross-partition scope jump
- **WHEN** rule-ir evaluates `jump_to(Relation::Owner)` on a province in "region_12"
- **THEN** the evaluator calls `query-engine.lookup_row("actor_state", actor_id)`
- **AND** the query-engine internally queries the `schema-registry`'s RelationGraph to resolve the target partition
- **AND** the query-engine performs an indexed lookup within that partition
- **THEN** the result is returned to the evaluator
