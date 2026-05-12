**Crate**: `scharnhorst_query`

## ADDED Requirements

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
1. Consult the table's `RowPositionMap` to translate `RowId → (batch_idx, offset)`
2. Scan across ALL record batches (not just `batches[0]`) to find the target batch
3. Return `RowNotFound` error if the RowId has been deleted or does not exist
4. All read operations MUST route through the `query-engine` (DC-10 compliance)

#### Scenario: Evaluator fetches a column by RowId
- **WHEN** rule-ir evaluates `Column("actor_state", "treasury")` in scope of `RowId(5)`
- **THEN** the evaluator calls `query_engine.lookup_row("actor_state", RowId(5))?.get_i64("treasury")` — NOT `column_view("actor_state", "treasury").get(5)`

#### Type definitions:
The `RowPositionMap` and `RowLookup` types SHALL be defined in `scharnhorst_core` and used by `scharnhorst_query`, `scharnhorst_rules`, and `scharnhorst_arrow_store`.

## MODIFIED Requirements

### (Compatibility) Requirement: Strict Read-Only Access (↔ journal-system, ↔ arrow-store)
The query engine SHALL operate exclusively on **immutable** `WorldSnapshot` references. It MUST NOT hold any mutable reference to Arrow tables and MUST NOT bypass the `journal-system` to write diffs.

Snapshots arrive at the query-engine via a **push model**: the `arrow_store` calls `ingest_snapshot(tick, table_name, batches)` to populate the engine's internal view cache. There is no pull-based `get_snapshot()` method on `QueryEngine`. Consumers never receive a full snapshot handle; instead, they access snapshot data through the query-engine's typed read APIs:

- `lookup_row(table, row_id)` for row-by-row access (DC-4)
- `column_view(table, column)` for direct columnar reads
- `read(request)` for batched multi-table access via `UnifiedReadSource`

#### Invariant:
For any query `Q` executed at generation N, `Q(snapshot_N)` returns the same result regardless of how many times it is called, and no side-effects are produced.

### (Compatibility) Requirement: Schema-Registry Dependency (↔ schema-registry)
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

### (Compatibility) Requirement: Unified Read Interface (↔ sim-scheduler, ↔ rule-ir, ↔ bevy-bridge)

The `query-engine` is the **sole read interface** for all world state consumers. All simulation systems, the `rule-ir` evaluator, and the `bevy-bridge` SHALL access the `WorldSnapshot` exclusively through the `query-engine` — whether via typed column access, relational queries, or SQL.

Internally, the `query-engine` may dispatch to different Arrow access strategies (direct column scan, dictionary-indexed lookup, DataFusion SQL plan) depending on the query type. This allows the Arrow layer to perform parallel optimizations (e.g., SIMD column scans, predicate pushdown) without the caller needing to know the strategy used.

#### Invariant:
No component outside the `query-engine` may hold a direct reference to an Arrow `RecordBatch` or `Table` from the snapshot. All reads go through the query-engine API.

> **Note on snapshot delivery**: The query-engine does **not** expose a pull-based `get_snapshot()` method. Internally, it caches `TableReadView` entries that are pushed to it by the `arrow_store` via `ingest_snapshot(tick, table_name, batches)`. The cached views are keyed by table name and indexed by the snapshot tick stored in `latest_tick()`. Consumers access data through the typed read APIs (`lookup_row`, `column_view`, `read`) rather than by obtaining a snapshot handle.

#### Scenario: Simulation system reads via query-engine
- **WHEN** the `EconomySystem` needs the treasury of actor FRA
- **THEN** it calls `query_engine.get_column("actor_state", "treasury", Filter::Eq("actor_id", FRA))` — the query-engine resolves the schema via the `schema-registry`, locates the correct partition, and returns a typed column reference.

### (Compatibility) Requirement: Debug-Write Isolation (↔ journal-system, ↔ sim-scheduler)
In debug builds, a `DebugWriteJournal` MAY be enabled that translates SQL `UPDATE`/`INSERT` statements into journal diffs. This path is:
- **Disabled** in production builds.
- **Disabled** in multiplayer sessions (to preserve determinism guarantees).
- Still routes through the journal, so it does not violate the single-write-entry-point invariant.

**Implementation**: The `DebugWriteJournal` intercepts SQL write statements from developer tooling, translates them into `Diff` objects, and submits them to the `journal-system`. The actual commit occurs at the next tick boundary (triggered by `sim-scheduler`), preserving the tick-aligned commit cycle.
