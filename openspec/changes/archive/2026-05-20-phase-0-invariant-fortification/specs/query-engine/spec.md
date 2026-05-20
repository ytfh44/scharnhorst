## MODIFIED Requirements

### Requirement: Unified Read Interface (sim-scheduler, rule-ir, bevy-bridge)
The `query-engine` is the sole simulation read interface for all world state consumers. All simulation systems, the `rule-ir` evaluator, and the `bevy-bridge` SHALL access Authority state exclusively through `query-engine`, whether via typed column access, row lookup, relational queries, batched reads, or SQL SELECT queries.

Consumers MUST NOT hold direct references to mutable `ArrowStore`, raw Arrow `RecordBatch` values, Arrow table internals, partition internals, or the schema-registry `RelationGraph`. Any snapshot-like access exposed by `query-engine` SHALL be read-only and SHALL NOT expose mutation APIs or raw store ownership.

Internally, the `query-engine` may dispatch to different Arrow access strategies depending on query type. This is an implementation detail hidden from consumers.

**Invariant:**
No component outside `query-engine` may hold a direct reference to an Authority Arrow table from the committed snapshot. All simulation reads go through the query-engine API.

#### Scenario: Simulation system reads via query-engine
- **WHEN** the Economy system needs the treasury of actor FRA
- **THEN** it calls a query-engine typed read or lookup API
- **AND** it does not access `ArrowStore` or raw Arrow arrays directly

#### Scenario: Bevy materialization obtains read-only data
- **WHEN** the Bevy bridge materializes visible province entities
- **THEN** it obtains read-only committed data through query-engine APIs
- **AND** it cannot mutate Authority state through the obtained handle

### Requirement: Strict Read-Only Access (journal-system, arrow-store)
The query engine SHALL operate exclusively on immutable committed Authority views. It MUST NOT hold mutable references to Arrow tables and MUST NOT bypass `journal-system` to write diffs.

Snapshots arrive at the query-engine via a push model from the journal commit path. Consumers access data through typed read APIs:

- `lookup_row(table, row_id)` for row-by-row access.
- `column_view(table, column)` for direct columnar reads.
- `batch_reader(table)` for table-local scans.
- `read(request)` for batched multi-table access through `UnifiedReadSource`.
- SQL SELECT/debug read APIs where enabled.

SQL write statements, where enabled in debug builds, MUST be translated into journal submissions and MUST NOT mutate query-engine caches or Arrow storage directly.

#### Scenario: Query returns no side effects
- **WHEN** a query reads the current actor treasury column
- **THEN** the query returns data for the committed generation
- **AND** no Authority table, journal diff queue, or schema registry is mutated

#### Scenario: SQL write is intercepted
- **WHEN** debug SQL includes an `INSERT`, `UPDATE`, or `DELETE`
- **THEN** the query-engine write path routes through `DebugWriteJournal`
- **AND** no direct cache or store mutation is performed

## ADDED Requirements

### Requirement: Snapshot Handle Hygiene
Query-engine SHALL expose a read-only `snapshot()` method returning a generation-tagged, read-only handle for materialization and tooling. This handle SHALL be incapable of reaching `ArrowStore` mutation methods. The handle SHALL NOT be the primary read path for simulation systems — systems SHALL use typed read APIs (`lookup_row`, `column_view`, `batch_reader`, `read`). The `snapshot()` method exists only for Bevy materialization, inspector tooling, and save/load reconstruction.

#### Scenario: Snapshot handle is read-only
- **WHEN** a consumer obtains a snapshot handle via `query_engine.snapshot()`
- **THEN** the handle provides read-only access to Arrow data for the latest committed generation
- **AND** the handle cannot be used to mutate Authority tables, create tables, or apply diffs

### Requirement: View Cache Ingestion Ordering Guarantee
`ingest_snapshot` SHALL store `latest_tick` via `Release` ordering before inserting into `view_cache`. Readers SHALL load `latest_tick()` via `Acquire` ordering before reading `view_cache`. This ordering pair ensures that a reader observing tick T also sees all `view_cache` entries written before that tick's store.

Concurrent `ingest_snapshot` calls for the same table name during multi-table ingestion SHALL be serialized by the outer commit path. The `view_cache` write lock provides mutual exclusion for snapshot data writes, but the `latest_tick` store occurs outside that lock — a reader observing the tick before the final table's cache insertion may see a partial view. This is harmless because all ingestion occurs within a single sequential `journal.commit()` call before `REFRESH_SIGNAL` broadcast.

**Normative requirement (I-QE-TICK-ORDERING):** Every reader method (`lookup_row`, `column_view`, `row_cursor`, `batch_reader`, `read`) SHALL call `latest_tick()` before accessing `view_cache`. The ordering MUST be: acquire tick (Acquire), then acquire cache (read lock). This is asserted by the View Cache Ingestion Ordering Guarantee above. Any reader that skips this ordering may observe stale data attributed to the wrong tick.

`ingest_snapshot` SHALL reject `Tick(u64::MAX)` as a sentinel frame to prevent accidental ingestion of non-commit markers into the view cache.

#### Scenario: Sentinel tick rejected by ingest_snapshot
- **WHEN** `ingest_snapshot` is called with `Tick(u64::MAX)` as the snapshot tick
- **THEN** the call returns an error describing the sentinel rejection
- **AND** no data is written to the view cache

#### Scenario: Reader observes consistent tick and cache
- **WHEN** a reader loads `latest_tick()` before `view_cache`
- **THEN** the cache data visible includes all tables ingested up to that tick
- **AND** after `REFRESH_SIGNAL`, all tables for the new tick are observable

#### Scenario: Reader methods maintain tick-then-cache ordering
- **WHEN** any `column_view()`, `row_cursor()`, `batch_reader()`, or `read()` call executes
- **THEN** it calls `latest_tick()` via Acquire before acquiring `view_cache` read lock
- **AND** the returned data is tagged with the tick observed at the time of the read

### Requirement: Schema Registry Mutation Goes Through Inspector
`QueryEngine::register_table_schema()` SHALL synchronize schema changes with the `InspectorConsole`. Each call to `register_table_schema()` SHALL register the `TableSpec` in the schema registry and then update the inspector console with the new table metadata. The raw `schema_registry_mut()` accessor is `pub(crate)` and returns a bare write lock guard; inspector synchronization is the caller's responsibility. `register_table_schema()` is the sole public entry point that bundles both operations.

#### Scenario: Registering a table spec through query engine
- **WHEN** `query_engine.register_table_schema(spec)` is called
- **THEN** the schema is registered in the schema registry
- **AND** the inspector console is updated with the new table metadata
- **AND** direct access to `schema_registry_mut()` that bypasses the inspector is unavailable

### Requirement: Derived Rebuild Reads Through Query Engine
Derived state rebuilds SHALL read Authority state through query-engine APIs. Derived rebuild code MUST NOT read Arrow store internals directly.

#### Scenario: Derived heatmap rebuild
- **WHEN** a map heatmap rebuilds after a tick commit
- **THEN** it reads population and owner data through query-engine APIs
- **AND** it stores the rebuilt heatmap outside Authority state

### Requirement: TicksMismatch Error Variant
`read()` SHALL return `QueryError::TicksMismatch { requested, actual }` when a non-zero tick in a `ReadRequest` does not match the cache tick, rather than using the generic `InvalidQuery` variant. This allows callers to pattern-match on tick synchronisation failures specifically.

#### Scenario: Read with mismatched tick returns TicksMismatch
- **WHEN** a `ReadRequest` with a non-zero tick that does not equal `latest_tick()` is submitted to `read()`
- **THEN** `QueryError::TicksMismatch` is returned with the requested and actual tick values

### Implementation Notes

- `register_table_schema`: acquires schema_registry lock before inspector lock to minimize the window between lock acquisition and first mutation. Inspector `register_schema` is infallible, so no rollback is needed.
- `inspect_table_summary`, `inspect_table_page`, `cached_table_names`: each calls `latest_tick()?` before accessing `view_cache` for tick-before-cache ordering.
