## Why

Phase 0 made determinism enforceable by the type system. Now Phase 1 must make that determinism **observable**. Currently, the `QueryEngine` is the designated read interface on paper but consumers still hold direct `WorldSnapshot` references. Debug inspection requires grepping save files or printf-debugging. There is zero runtime visibility into scheduler timing, query latency, or diff volume. Without observability, Phase 2's rule diagnostics and Phase 4's proof system lack the instrumentation substrate they need.

## What Changes

- **BREAKING**: Audit all read paths across `sim-scheduler`, `rule-ir`, `bevy-bridge`, and `save-system`. Route every consumer through `QueryEngine` typed APIs. Remove all public `WorldSnapshot::table()` or `snapshot().arrow_table()` accessors.
- **BREAKING**: `QueryEngine::snapshot()` returns a read-only borrowed view (not the raw snapshot struct). This is the only way to obtain a point-in-time view outside typed read APIs.
- Add `QueryEngine::validate()` that checks all registered `RulePath` entries against the current schema at load time — fails early, not at first rule evaluation.
- Expose `QueryEngine::sql(&self, sql: &str) -> Result<RecordBatch>` for ad-hoc SELECT queries against the current snapshot via DataFusion. Debug builds only.
- Build a `bevy_egui` inspector panel system: table viewer (paginated, sortable, filterable), relation graph visualizer, diff inspector (before/after per tick), and snapshot browser (compare ticks, state hash differences).
- Instrument the scheduler: per-phase timing, per-system timing, diff counts per tick.
- Instrument the query engine: query latency histogram, cache hit ratio, most-accessed tables/columns.
- Instrument the journal: commit latency, diff batch size distribution.
- Export all metrics via `tracing` (already a dependency) to a live console or file.

## Capabilities

### New Capabilities

- `inspector-tooling`: Bevy egui debug panels — table browser, relation graph visualizer, diff inspector, snapshot browser. Available only in debug builds. Reads through `query-engine`, never writes Authority state.
- `runtime-telemetry`: Instrumentation hooks across scheduler, query engine, and journal. Metrics exported via `tracing`. All instrumentation follows zero-cost-when-disabled pattern; production builds compile out all measurement.

### Modified Capabilities

- `query-engine`: All simulation read paths route exclusively through `QueryEngine`. Raw `WorldSnapshot::table()` and `snapshot().arrow_table()` accessors removed from public API. `validate()` added for early schema-path checking. `sql()` added for ad-hoc DataFusion queries.
- `bevy-bridge`: Adds inspector panel system (debug builds) — table viewer, diff stream, rule debugger, modifier chain inspector, relation graph visualizer, performance dashboard, snapshot timeline. All panels read through `query-engine`.
- `sim-scheduler`: Per-phase and per-system timing instrumentation added. Diff count tracking per tick.
- `journal-system`: Commit latency and diff batch size instrumentation added.

## Impact

- Affected crates: `scharnhorst_query`, `scharnhorst_arrow_store`, `scharnhorst_bevy`, `scharnhorst_scheduler`, `scharnhorst_journal`, `scharnhorst_rules`, `scharnhorst_save`, `scharnhorst_integration_tests`.
- Public API impact: Any code path currently calling `snapshot().arrow_table()`, `WorldSnapshot::table()`, or directly accessing Arrow internals must migrate to `QueryEngine` typed APIs (`lookup_row`, `column_view`, `batch_reader`, `read`).
- New dev dependency: `bevy_egui` for inspector panels (debug builds only).
- No data format changes. Authority snapshot schemas unchanged.
- Production build impact: All inspector panels and telemetry compile out. No runtime overhead.