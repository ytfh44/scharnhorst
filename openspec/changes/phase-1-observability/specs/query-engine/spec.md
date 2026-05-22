## ADDED Requirements

### Requirement: Schema-Path Validation at Load Time
`QueryEngine::validate()` SHALL check all registered `RulePath` entries against the current schema at content-load time. Validation SHALL fail early — before the first tick — with actionable error messages listing each invalid path, the expected table/column, and the actual schema state.

#### Scenario: Valid paths pass validation
- **WHEN** `query_engine.validate()` is called after all content is compiled and schema is frozen
- **THEN** all `RulePath` entries are checked against the `SchemaRegistry`
- **AND** if all table and column references are valid, `Ok(())` is returned

#### Scenario: Missing table produces validation error
- **WHEN** a `RulePath` references table `nonexistent_table` that is not registered in the `SchemaRegistry`
- **THEN** `validate()` returns a `ValidationError` with `kind: MissingTable`, the path, the invalid table name, and a descriptive message listing available tables

#### Scenario: Missing column produces validation error
- **WHEN** a `RulePath` references column `missing_col` on a valid table that does not have that column
- **THEN** `validate()` returns a `ValidationError` with `kind: MissingColumn`, the path, table name, column name, and all available columns for that table

#### Scenario: Invalid relation chain produces validation error
- **WHEN** a `RulePath` includes a relation that does not exist in the `RelationGraph`
- **THEN** `validate()` returns a `ValidationError` with `kind: MissingRelation`, the path, the invalid relation name, and available relations from the source table

#### Scenario: validate() on empty RulePath set
- **WHEN** `query_engine.validate()` is called before any `RulePath` entries have been registered (e.g., content loading in progress with no rules compiled yet)
- **THEN** `validate()` returns `Ok(())` trivially
- **AND** no spurious warnings or errors are emitted for the empty set

#### Scenario: validate() called multiple times
- **WHEN** `query_engine.validate()` is called, then called again with no schema or RulePath changes in between
- **THEN** both calls return identical results — `validate()` is idempotent
- **AND** the second call does not recompute relations and RulePath lookups on a different code path

#### Scenario: validate() called during simulation
- **WHEN** a developer invokes `query_engine.validate()` at tick 10 via the debug inspector, after rules have been evaluated for 10 ticks
- **THEN** `validate()` re-checks all RulePaths against the current (frozen) schema
- **AND** since schema is frozen and RulePaths don't change mid-simulation, the result is identical to the load-time validation result
- **AND** `validate()` does not access or modify any per-tick state (diffs, snapshots, journal records)

#### Scenario: validate() on table with zero rows
- **WHEN** a `RulePath` references a table `events` that exists in schema, has all expected columns, but has zero rows
- **THEN** `validate()` succeeds — it checks schema structural correctness (table exists, column exists, relation exists), not data-level correctness (rows exist, values are non-null)
- **AND** row-level errors (e.g., `RowNotFound` from `lookup_row`) are deferred to evaluation time

### Requirement: SQL Console for Debug Queries
`QueryEngine::sql()` SHALL run ad-hoc DataFusion SELECT queries against the current snapshot's committed data. Only SELECT statements are permitted. Available only in debug builds.

`sql()` SHALL create a **fresh DataFusion `SessionContext`** per invocation. No session state (temporary tables, session variables, cached query plans) SHALL persist between calls. Each invocation registers current `view_cache` tables fresh and discards the context afterward.

`sql()` SHALL reject non-SELECT statements (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER). The rejection SHALL occur before DataFusion execution — the statement text is parsed to detect prohibited keywords, not executed-then-rolled-back.

#### Scenario: Simple SELECT query
- **WHEN** `query_engine.sql("SELECT * FROM actor_state")` is called
- **THEN** a fresh `SessionContext` is created
- **AND** all current `view_cache` tables are registered in it
- **THEN** the query executes and a `RecordBatch` with all rows from `actor_state` is returned
- **AND** the `SessionContext` is discarded

#### Scenario: SELECT with WHERE filter
- **WHEN** `query_engine.sql("SELECT name, treasury FROM actor_state WHERE treasury > 1000")` is called
- **THEN** only rows matching the filter are returned
- **AND** only the requested columns are included in the result

#### Scenario: Non-SELECT statement rejected
- **WHEN** `query_engine.sql("INSERT INTO actor_state VALUES (1, 'test')")` is called
- **THEN** the call returns `QueryError::SqlNotAllowed` with a message indicating only SELECT is permitted
- **AND** no `SessionContext` is created — rejected before DataFusion execution
- **AND** no data is written to any table

#### Scenario: CREATE TABLE AS rejected (side-channel DDL)
- **WHEN** `query_engine.sql("CREATE TABLE foo AS SELECT * FROM actor_state")` is called
- **THEN** the call returns `QueryError::SqlNotAllowed`
- **AND** no table `foo` is created in DataFusion session state (no session state exists to leak)
- **AND** a subsequent `SELECT * FROM foo` also returns `QueryError::SqlNotAllowed` (not, critically, a DataFusion execution error that reveals the ghost table)

#### Scenario: Session state does not leak between calls
- **WHEN** `query_engine.sql("SELECT 1")` is called, then `query_engine.sql("SELECT * FROM foo")` is called
- **THEN** the second call does NOT see any table `foo` from the first invocation
- **AND** each call operates in an independent `SessionContext` that is discarded after the call completes

#### Scenario: SQL on unregistered table
- **WHEN** `query_engine.sql("SELECT * FROM nonexistent")` is called
- **THEN** the call returns `QueryError::SqlExecutionError` with the DataFusion error message

#### Scenario: SQL called mid-commit (partial snapshot observation)
- **WHEN** during `journal.commit()` a multi-table snapshot is being ingested, and concurrently an external thread (inspector UI) calls `sql("SELECT * FROM table_a, table_b")`
- **THEN** the SQL query MAY observe an intermediate state where `table_a` is at tick N but `table_b` is still at tick N-1
- **AND** this is documented as acceptable: debug tooling operates on best-effort consistency; the window is bounded by the sequential commit duration (microseconds)
- **AND** no error is returned — DataFusion executes against whatever tables are currently in `view_cache`
- **NOTE**: This scenario only applies to debug tooling called from a non-simulation thread. Simulation systems running on the scheduler thread never observe partial snapshots because they execute sequentially between commits.

#### Scenario: SQL with empty string
- **WHEN** `query_engine.sql("")` is called
- **THEN** the call returns `QueryError::SqlParseError` or `QueryError::SqlNotAllowed` with a descriptive message

#### Scenario: SQL with semicolon-separated multi-statement
- **WHEN** `query_engine.sql("SELECT 1; SELECT 2")` is called
- **THEN** only the first statement is executed; the second is silently ignored, OR the entire multi-statement string is rejected with `QueryError::SqlNotAllowed`
- **DECISION**: Reject multi-statement strings. `sql()` accepts exactly one SELECT statement.

#### Scenario: SQL cross-join causing large result
- **WHEN** `query_engine.sql("SELECT * FROM actor_state CROSS JOIN province_data")` is called on tables with 1000 and 500 rows
- **THEN** DataFusion attempts to produce a 500,000-row result set
- **AND** the system may exhaust memory (OOM)
- **NOTE**: SQL console is developer-only debug tooling. OOM risk is acceptable for Phase 1. Memory limits are deferred to Phase 5.

### Requirement: WorldView — Frozen Point-in-Time Snapshot Handle
`QueryEngine::snapshot()` SHALL return a `WorldView` handle, not a raw `WorldSnapshot` or `Arc<ArrowStore>` reference. `WorldView` is a point-in-time, generation-tagged, read-only snapshot handle constrained to the latest committed generation.

`WorldView` SHALL internally hold `Arc<WorldSnapshot>` to ensure data remains valid even after the `ArrowStore` truncates old snapshots. The reference count prevents deallocation while any `WorldView` references the snapshot.

`WorldView` SHALL provide: `tick()`, `table_names()`, `row_count(table)`, `column_names(table)`, `column_type(table, column)`, and `iter_rows(table)` with typed getter methods per cell.

`WorldView` SHALL NOT expose: `RecordBatch`, `arrow_schema::Schema`, Arrow array accessors, `ArrowStore` internals, partition metadata, or any mutation API.

#### Scenario: WorldView obtained from query-engine
- **WHEN** a consumer calls `query_engine.snapshot()`
- **THEN** a `WorldView` is returned containing the generation number and `Arc<WorldSnapshot>` for the latest committed tick
- **AND** the handle provides only read-only table iteration

#### Scenario: WorldView held across tick boundary (stale data)
- **WHEN** consumer A calls `query_engine.snapshot()` at tick 5, obtaining `WorldView(tick=5)`
- **AND** the simulation commits tick 6, then ticks 7 through 50
- **WHEN** consumer A still holds `WorldView(tick=5)` and calls `world_view.row_count("actor_state")`
- **THEN** the result reflects data at tick 5 — the `WorldView` is a frozen point-in-time snapshot
- **AND** `world_view.tick()` returns `5`, while `query_engine.latest_tick()` returns `50`
- **AND** consumer A can detect staleness by comparing these tick values
- **AND** consumer A can obtain fresh data by calling `query_engine.snapshot()` again

#### Scenario: WorldView valid after checkpoint truncation
- **WHEN** `ArrowStore` truncates snapshots older than tick 60 (i.e., ticks 1-59 removed)
- **AND** consumer B holds `WorldView(tick=50)` (a now-truncated snapshot)
- **WHEN** consumer B calls `world_view.row_count("actor_state")`
- **THEN** the call succeeds — `WorldView`'s internal `Arc<WorldSnapshot>` keeps the snapshot alive
- **AND** the returned data is from tick 50, consistent and valid
- **AND** `WorldView` continues to work until consumer B drops the handle

#### Scenario: WorldView on empty table
- **WHEN** consumer calls `world_view.iter_rows("empty_table")` on a table with zero rows
- **THEN** the iterator produces zero items immediately
- **AND** `world_view.row_count("empty_table")` returns `0`
- **AND** `world_view.column_names("empty_table")` returns the column names as usual

#### Scenario: WorldView on non-existent table
- **WHEN** consumer calls `world_view.row_count("nonexistent_table")`
- **THEN** the call returns `QueryError::TableNotFound` with the table name
- **AND** the error message includes the list of available table names

#### Scenario: WorldView column reader with wrong type panics in debug
- **WHEN** consumer calls `world_view.iter_rows("actor_state")` and attempts to read column `treasury` (Int64) as a String
- **THEN** in debug builds, the typed getter returns an error (`QueryError::TypeMismatch`)
- **AND** in release builds, the behavior is the same (error, not panic)
- **NOTE**: Arrow array access is type-checked; no `unwrap()` on Arrow internal type assertions

### Requirement: Unified Read Interface — Audience Expansion
The `query-engine` is the sole simulation read interface for all world state consumers. This requirement is strengthened from Phase 0: not only must simulation systems go through `query-engine`, but the `bevy-bridge`'s materialization, inspector panels, save/load reconstruction (simulation-phase only), and **any code that previously held `Arc<WorldSnapshot>` directly** MUST use `WorldView` from `query_engine.snapshot()`.

#### Scenario: Consumer cannot hold raw WorldSnapshot
- **WHEN** a crate attempts to store a field of type `WorldSnapshot` or `Arc<WorldSnapshot>`
- **THEN** the type system prevents it — `WorldSnapshot` and its Arc wrapper are not importable by external crates
- **AND** the consumer must use `WorldView` from `query_engine` instead

#### Scenario: Consumer cannot access RecordBatch
- **WHEN** any crate outside `scharnhorst_arrow_store` attempts to call `.arrow_table()` or access a `RecordBatch` from snapshot data
- **THEN** compilation fails — those methods are `pub(crate)` or removed from the public API

### Requirement: Snapshot Handle Hygiene
Query-engine SHALL expose a read-only `snapshot()` method returning `WorldView`. The handle SHALL be incapable of reaching `ArrowStore` mutation methods, raw `RecordBatch`, Arrow schema, or partition internals.

`WorldView` is NOT the primary read path for simulation systems. Simulation systems SHALL use typed read APIs (`lookup_row`, `column_view`, `batch_reader`, `read`). `snapshot()` exists only for Bevy materialization, inspector tooling, and save/load reconstruction.

#### Scenario: WorldView is read-only
- **WHEN** a consumer obtains a `WorldView` via `query_engine.snapshot()`
- **THEN** the handle provides read-only table iteration for the generation at which it was obtained
- **AND** the handle cannot be used to apply diffs, create tables, register types, or access `ArrowStore` mutation methods
- **AND** the handle does not expose `RecordBatch`, `arrow_schema::Schema`, or raw Arrow arrays

#### Scenario: Inspector iterates table via WorldView
- **WHEN** the Table Viewer panel needs to display rows from `actor_state`
- **THEN** it calls `world_view.iter_rows("actor_state")` and reads cell values via typed getters
- **AND** it does not access `RecordBatch`, Arrow arrays, or partition internals

### ADDED Invariants

#### Invariant: WorldView Immutability (I-WV-IMMUTABLE)
WorldView is a frozen point-in-time snapshot. Its data SHALL NOT change after construction. A `WorldView` obtained at tick T SHALL always return data from tick T, even if the consumer holds it across subsequent ticks and the underlying `ArrowStore` has advanced to tick T+N. This invariant ensures consumers do not observe phantom diffs or partially-applied state.

#### Invariant: WorldView Arc-Backing (I-WV-ARC-BACKED)
`WorldView` SHALL internally hold `Arc<WorldSnapshot>`. The `WorldView` SHALL remain valid even after the `ArrowStore` truncates snapshots older than the retention threshold. The `Arc` reference counting ensures the `WorldSnapshot` is not deallocated while any `WorldView` holds a reference. Consumers SHALL NOT implement their own lifetime management for `WorldView` references; the `Arc` handles it correctly.

#### Invariant: SQL Fresh Session (I-SQL-FRESH-SESSION)
`sql()` SHALL create a fresh DataFusion `SessionContext` per invocation. No session state — temporary tables, session variables, cached query plans, UDFs — SHALL persist between calls. This prevents DDL and DML side effects from accumulating across multiple `sql()` invocations, guaranteeing that each call observes a clean, read-only view of the current snapshot.

#### Invariant: SQL Debug Consistency (I-SQL-DEBUG-CONSISTENCY)
`sql()` and inspector panel reads MAY observe partially-ingested snapshots during the microsecond window of multi-table ingestion in `journal.commit()`. This is acceptable because debug tooling operates on best-effort consistency and the window is bounded by the sequential commit duration. Simulation systems running on the scheduler thread never observe partial snapshots because they execute sequentially between commits. This invariant distinguishes debug-level consistency guarantees from simulation-level guarantees.