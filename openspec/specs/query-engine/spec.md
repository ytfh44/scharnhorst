## Requirements

### Requirement: Low-Latency Row Access
The system SHALL provide an API for direct, typed access to Arrow columns for hot-path simulation.

#### Scenario: Summing treasury
- **WHEN** a system needs the total gold of all actors
- **THEN** it accesses the "treasury" column as a `PrimitiveArray<i64>` and sums using SIMD.

### Requirement: Relational Querying
The system MUST allow filtering and grouping of state based on relations (e.g., "find all pops in provinces owned by actor X").

#### Scenario: Filtered scan
- **WHEN** querying for populations in "France"
- **THEN** the engine joins "population_location" and "ownership_relation" tables to return the result.

### Requirement: SQL Integration
The system SHALL provide an optional SQL interface via DataFusion for complex analysis and developer tooling.

#### Scenario: Complex report
- **WHEN** running a debug query `SELECT SUM(pop) FROM pops GROUP BY culture`
- **THEN** the system executes the SQL against the current snapshot and returns a result table.

### Requirement: Row-by-Row Access via `lookup_row()`
The system SHALL expose a `lookup_row(table, row_id)` method on `QueryEngine` as the **sole mechanism** for single-row access. `RowId.0` MUST NOT be used directly as an array index.

#### API Contract
```rust
fn lookup_row(&self, table: &str, row_id: RowId) -> QueryResult<RowLookup>;
```

Where `RowLookup` provides typed column access for the identified row:
```rust
pub struct RowLookup { /* batch_idx, offset within batch */ }
impl RowLookup {
    fn get_i64(&self, column: &str) -> QueryResult<Option<i64>>;
    fn get_f64(&self, column: &str) -> QueryResult<Option<f64>>;
    fn get_bool(&self, column: &str) -> QueryResult<Option<bool>>;
    fn get_string(&self, column: &str) -> QueryResult<Option<String>>;
}
```

#### Implementation requirements:
1. Consult the table's `RowPositionMap` to translate `RowId -> (batch_idx, offset)`
2. Scan across ALL record batches (not just `batches[0]`) to find the target batch
3. Return `RowNotFound` error if the RowId has been deleted or does not exist
4. All read operations MUST route through the `query-engine`

#### Scenario: Evaluator fetches a column by RowId
- **WHEN** rule-ir evaluates `Column("actor_state", "treasury")` in scope of `RowId(5)`
- **THEN** the evaluator calls `query_engine.lookup_row("actor_state", RowId(5))?.get_i64("treasury")` -- NOT `column_view("actor_state", "treasury").get(5)`

#### Type definitions:
The `RowPositionMap` and `RowLookup` types SHALL be defined in `scharnhorst_core` and used by `scharnhorst_query`, `scharnhorst_rules`, and `scharnhorst_arrow_store`.

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

### Requirement: Schema-Registry Dependency (schema-registry)
The query engine SHALL query the `schema-registry` at load time to resolve table schemas, field semantic types, and relation definitions. This metadata is cached and refreshed only when the schema-registry changes (i.e., during cold-start content loading).

**RelationGraph Access**: The `query-engine` is the **sole consumer** of the `schema-registry`'s `RelationGraph`. All cross-partition lookups and relation-based queries flow through the `query-engine`:
- Direct relation graph queries (for JOIN optimization)
- Cross-partition row lookups via `lookup_row(table, row_id)`
- Scope traversal for `rule-ir` (indirectly, via the unified read interface)

#### Scenario: Cross-partition lookup via RelationGraph
- **WHEN** a consumer calls `query_engine.lookup_row("actor_state", actor_id)`
- **THEN** the query-engine:
  1. Queries `schema-registry` for the table's partition key
  2. Consults the `RelationGraph` to determine the target partition
  3. Performs an indexed lookup within that partition
  4. Returns the result to the caller

#### Scenario: SQL query with semantic awareness
- **WHEN** a SQL query references "treasury" in "actor_state"
- **THEN** the engine uses the schema-registry to resolve the column's Arrow data type (`Int64`) and semantic (`FixedPoint { scale: 10000 }`), applying appropriate display formatting.

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

### Requirement: Debug-Write Isolation (journal-system, sim-scheduler)
In debug builds, a `DebugWriteJournal` MAY be enabled that translates SQL `UPDATE`/`INSERT` statements into journal diffs. This path is:
- **Disabled** in production builds.
- **Disabled** in multiplayer sessions (to preserve determinism guarantees).
- Still routes through the journal, so it does not violate the single-write-entry-point invariant.

**Implementation**: The `DebugWriteJournal` intercepts SQL write statements from developer tooling, translates them into `Diff` objects, and submits them to the `journal-system`. The actual commit occurs at the next tick boundary (triggered by `sim-scheduler`), preserving the tick-aligned commit cycle.

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

---

## Invariants

### I-QE-TICK-SENTINEL
`u64::MAX` is reserved as the None sentinel for `latest_tick`. No valid tick may have this value. `Tick::MAX = Tick(u64::MAX - 1)` encodes this at the type level. `Tick::next()` uses `saturating_add(1)`, so calling `Tick::MAX.next()` saturates to `Tick(u64::MAX)` — the sentinel. In normal operation this is physically unreachable (~585 billion years at 1 billion ticks/sec). A `debug_assert!` in `ingest_snapshot` guards against accidental sentinel ingestion from test code, deserialization, or replay.

### I-QE-TICK-ORDERING
`latest_tick.store(Release)` in `ingest_snapshot` is paired with `latest_tick.load(Acquire)` in `latest_tick()`. This ordering pair ensures that **when a reader calls `latest_tick()` (Acquire) BEFORE reading `view_cache`**, the `view_cache` insertion performed *in the `ingest_snapshot` call that stored T* is visible to that reader. The precondition is critical: a reader that reads `view_cache` first and `latest_tick()` second may observe stale cache data associated with a newer tick. All current reader sites in `lookup_row()`, `column_view()`, `row_cursor()`, and `batch_reader()` maintain the correct access order (tick first, then cache). During multi-table snapshot ingestion (a sequence of `ingest_snapshot` calls within the journal commit), each call stores tick T independently — intermediate stores expose T with a partial `view_cache` (only tables ingested so far). Since all ingestion and the final `store_world_snapshot` happen within a single sequential `atomic_commit()` phase, no concurrent reader observes the intermediate states.

### I-QE-READ-ONLY
The query-engine operates exclusively on immutable committed Authority views. No query, typed read, SQL SELECT, or snapshot access path mutates `ArrowStore` tables, `view_cache` entries, journal diff queues, or the schema registry. SQL write statements are intercepted by `DebugWriteJournal` and routed to journal submission — never applied directly.

### I-QE-INSPECTOR-SYNC
`QueryEngine::register_table_schema()` bundles schema registry registration with inspector console update atomically. The raw `schema_registry_mut()` accessor is `pub(crate)` and returns a bare write lock guard; inspector synchronization is the caller's responsibility. `register_table_schema()` is the sole public entry point that guarantees both operations occur together.

---

## Design Notes

### Tick Sentinel `u64::MAX` — Silent Data Corruption Prevention (CRITICAL)

**Location**: Snapshot Ingestion, I-QE-TICK-SENTINEL

**Problem**: `u64::MAX` is reserved as the None sentinel for `latest_tick`. `Tick` is a public newtype over `u64` with `pub u64` field access. Any code path — test code, checkpoint deserialization, deterministic replay, or future extensions — can construct `Tick(u64::MAX)`. If passed to `ingest_snapshot`, the atomic store of `u64::MAX` is indistinguishable from `None`. The result: `latest_tick()` returns `None`, and `lookup_row()` returns `Err("no tick available")` despite data being present.

Additionally, `Tick::next()` uses `saturating_add(1)`, so `Tick::MAX = Tick(u64::MAX - 1)` followed by `.next()` produces `Tick(u64::MAX)` — exactly the sentinel. While unreachable in normal operation, this path is formally possible.

**Impact**: Silent logical corruption — data exists but is invisible.

**Recommended Corrections Applied**:

1. Public const `Tick::MAX = Tick(u64::MAX - 1)` reserves the sentinel value at the type level.
2. In `ingest_snapshot`, a debug-only assertion guards against sentinel ingestion:
   ```rust
   debug_assert!(tick.as_u64() != u64::MAX, "Tick sentinel collision");
   ```
3. The sentinel invariant is documented on `latest_tick`:
   ```
   /// INVARIANT: u64::MAX is reserved as the None sentinel.
   /// Tick::as_u64() must never equal u64::MAX.
   ```

### `ingest_snapshot` Ordering — Fragile Stated Invariant (CRITICAL)

**Location**: Snapshot Ingestion, I-QE-TICK-ORDERING

**Problem**: I-QE-TICK-ORDERING claims:
> "Any reader observing tick T also sees all view_cache insertions performed prior to T's store."

This is achieved via `store(Release)` in `ingest_snapshot` paired with `load(Acquire)` in `latest_tick()`. However, the invariant **only holds if the reader loads `latest_tick` BEFORE reading `view_cache`**. A reader that reads `view_cache` first, then `latest_tick`, can observe:

1. Reader: acquires `view_cache.read()` → sees old data
2. Writer: completes both `cache.insert()` and `latest_tick.store(Release)`
3. Reader: `latest_tick.load(Acquire)` → sees new tick T
4. Reader: uses old cache data under the belief it corresponds to tick T

**Impact**: A future code path that reads cache before tick will silently attribute stale data to a newer tick.

**Recommended Corrections Applied**:

Documented the required access order on both methods:

On `ingest_snapshot`:
```
ORDERING INVARIANT: Readers MUST load `latest_tick()` BEFORE
acquiring the `view_cache` read lock. Violating this order may
cause stale cache data to be attributed to a newer tick.
```

Verified `lookup_row()` and all other reader sites maintain this order.

---

## Implementation Notes

### Atomic `latest_tick` Replacement

The `latest_tick` field changes from `Arc<RwLock<Option<Tick>>>` to `Arc<AtomicU64>`:

```rust
latest_tick: Arc<AtomicU64>  // u64::MAX represents None
```

**Sentinel encoding**:
- `None` is represented by `u64::MAX`
- `Tick::MAX = Tick(u64::MAX - 1)` reserves the sentinel at the type level
- `Tick::next()` uses `saturating_add`, so `Tick::MAX.next()` saturates to `Tick(u64::MAX)` — the sentinel

**Ordering pair**:
- `ingest_snapshot` uses `Release` on the store
- `latest_tick()` uses `Acquire` on the load

This ensures that when a reader calls `latest_tick()` (Acquire) BEFORE reading `view_cache`, the `view_cache` insertion performed in the `ingest_snapshot` call that stored T is visible to that reader.

### Tick Space Reduction

`u64::MAX` is reserved as the None sentinel. This reduces the usable tick range from `[0, 2^64-1]` to `[0, 2^64-2]`. At 1 billion ticks/second (impossibly fast), this is ~585 years of ticks. The reduction is harmless.

### Unchanged Fields

Fields using Mutex/RwLock:
- `schema_registry: Arc<RwLock<SchemaRegistry>>`
- `view_cache: Arc<RwLock<HashMap<String, TableReadView>>>`
- `latest_snapshot: Arc<RwLock<Option<Arc<WorldSnapshot>>>>`
- `debug_journal: Arc<DebugWriteJournal>` (debug builds only)
- `sql_context: Arc<RwLock<SqlExecutionContext>>`

