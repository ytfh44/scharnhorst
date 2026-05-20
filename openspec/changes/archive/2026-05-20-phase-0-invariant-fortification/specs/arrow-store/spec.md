## ADDED Requirements

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
