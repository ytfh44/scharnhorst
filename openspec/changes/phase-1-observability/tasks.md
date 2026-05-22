## 1. WorldView — Narrow Snapshot Handle

- [ ] 1.1 Define `WorldView` type in `scharnhorst_arrow_store` with `table_names()`, `row_count()`, `column_names()`, `column_type()`, `iter_rows()` with typed cell getters. Internal borrow of `Arc<WorldSnapshot>`, no public `RecordBatch` or Arrow accessors.
- [ ] 1.2 Add `QueryEngine::snapshot(&self) -> QueryResult<WorldView>` that returns a `WorldView` for the latest committed generation.
- [ ] 1.3 Add unit tests: `WorldView` cannot access `RecordBatch` or Arrow internals; `iter_rows()` produces correct typed values; `row_count()` matches snapshot.

## 2. Read Path Audit — Route All Consumers Through QueryEngine

- [ ] 2.1 Audit `scharnhorst_rules` evaluator and scope modules for any remaining direct `WorldSnapshot` or Arrow access. Migrate any found paths to `query-engine` APIs (`lookup_row`, `column_view`).
- [ ] 2.2 Audit `scharnhorst_bevy` `SyncState`, `ViewModel`, `materialization.rs`, and `sync.rs`. Replace `Arc<WorldSnapshot>` fields with `WorldView` from `query_engine.snapshot()`. Replace any direct Arrow access with `WorldView` iteration.
- [ ] 2.3 Audit `scharnhorst_save` `load_reconstruction.rs` and `snapshot_persistence.rs` for any simulation-phase `WorldSnapshot` direct access. Initialization-phase access via `InitToken` is already allowed — verify no simulation-phase access exists.
- [ ] 2.4 Make `WorldSnapshot::table()` and `WorldSnapshot::arrow_table()` `pub(crate)`. Verify no external crate calls them.
- [ ] 2.5 Update integration test harness to use `WorldView` for any test that inspects snapshot contents.

## 3. QueryEngine — Validate and SQL

- [ ] 3.1 Implement `QueryEngine::validate()` that iterates all registered `RulePath` entries, checks each table/column against `SchemaRegistry`, checks each relation against `RelationGraph`, and returns `Vec<ValidationError>` with path, invalid component, and message.
- [ ] 3.2 Add `ValidationError` type to `scharnhorst_query::error` with path, context (table/column/relation), and message fields.
- [ ] 3.3 Implement `QueryEngine::sql(&self, sql: &str) -> QueryResult<RecordBatch>` that runs SELECT-only queries against `view_cache` via DataFusion. Reject non-SELECT statements with `QueryError::SqlNotAllowed`.
- [ ] 3.4 Add unit tests: `validate()` passes on valid schema; `validate()` catches missing table/column/relation; `sql()` returns correct SELECT results; `sql()` rejects INSERT/UPDATE/DELETE.

## 4. Read Path Removal — Clean Up Remaining Public Accessors

- [ ] 4.1 Verify `ArrowStore` public API no longer exposes raw `RecordBatch` or table internals to simulation consumers. Mutation APIs already behind token guards from Phase 0 — verify no read-side leaks.
- [ ] 4.2 Remove or deprecate any remaining public methods that return `RecordBatch`, `Schema`, or `ArrowTable` directly. Make them `pub(crate)` if still needed internally.
- [ ] 4.3 Add compile-fail tests or boundary integration tests proving simulation consumers cannot access `WorldSnapshot` or `RecordBatch` directly.

## 5. Telemetry — Scheduler Instrumentation

- [ ] 5.1 Add `metrics` feature flag to `scharnhorst_scheduler/Cargo.toml`.
- [ ] 5.2 Add `#[cfg(feature = "metrics")]` instrumentation module in `scharnhorst_scheduler/src/telemetry.rs` with helpers for timing spans.
- [ ] 5.3 Instrument `scheduler.rs` `tick()`: enter `scheduler_tick` span at tick start; enter `scheduler_phase` span per phase with `tick` and `phase` fields; exit after phase systems complete.
- [ ] 5.4 Instrument per-system execution: `tracing::debug!` event per system with `tick`, `phase`, `system_name`, `duration_micros`.
- [ ] 5.5 Add diff count tracking: accumulate diffs from journal after commit; emit `tracing::info!` event with `tick` and `diff_count`.
- [ ] 5.6 Add unit tests (with metrics enabled): verify spans/events emitted with correct fields; verify no events when metrics disabled.

## 6. Telemetry — Query Engine Instrumentation

- [ ] 6.1 Add `metrics` feature flag to `scharnhorst_query/Cargo.toml`.
- [ ] 6.2 Add `#[cfg(feature = "metrics")]` instrumentation to typed read APIs: `tracing::debug!` event per call with `method`, `table`, `column` (where applicable), `duration_micros`.
- [ ] 6.3 Add cache hit/miss counters behind `AtomicU64`. Increment on each `lookup_row`/`column_view` call. Emit summary event at tick end with `tick`, `cache_hits`, `cache_misses`, `hit_ratio`.
- [ ] 6.4 Add access pattern tracking: `HashMap<String, u64>` for table/column access counts, reset at tick start via refresh signal. Emit summary at tick end.
- [ ] 6.5 Add unit tests: verify latency events emitted; verify cache metrics computed correctly; verify access counts increment per call.

## 7. Telemetry — Journal Instrumentation

- [ ] 7.1 Add `metrics` feature flag to `scharnhorst_journal/Cargo.toml`.
- [ ] 7.2 Instrument `journal.rs` `commit()`: enter `journal_commit` span at start, close after commit completes. On success: `tracing::info!` with `tick`, `diff_count`, `duration_micros`. On failure: `tracing::warn!` with same fields plus `error`.
- [ ] 7.3 Add `diff_count` event: `tracing::debug!` per commit with `tick` and `diff_count`.
- [ ] 7.4 Verify `CommitRecord` unchanged — no telemetry data persisted. Add test proving replay with metrics produces identical hashes.
- [ ] 7.5 Add unit tests: verify span/events emitted; verify no events when metrics disabled.

## 8. Inspector Panels — Core Infrastructure

- [ ] 8.1 Add `bevy_egui` as optional dev dependency in workspace `Cargo.toml`.
- [ ] 8.2 Create `InspectorPlugin` in `scharnhorst_bevy/src/inspector/` with `#[cfg(debug_assertions)]` gating on all modules and the plugin registration.
- [ ] 8.3 `InspectorPlugin::build()` registers egui window systems for each panel, obtains `query_engine` handle from Bevy resources, and wires panel state.
- [ ] 8.4 Define `InspectorState` resource holding current table selection, page number, sort column/direction, filter string, diff history buffer, and snapshot comparison selections.
- [ ] 8.5 Add unit tests: `InspectorPlugin` registers without panic; panel systems obtain data from `query_engine` without panicking on empty world.

## 9. Inspector Panels — Table Viewer

- [ ] 9.1 Implement `TableWindow::table_dropdown()` displaying all registered table names from `query_engine`.
- [ ] 9.2 Implement `TableWindow::schema_display()` showing column names and Arrow data types for the selected table.
- [ ] 9.3 Implement `TableWindow::data_table()` with paginated, sortable, filterable row display. Rows obtained via `WorldView::iter_rows()`. Page size configurable, default 50.
- [ ] 9.4 Add integration test: select a table, verify schema displayed, verify row count matches, paginate forward and back, sort by column, filter by value.
- [ ] 9.5 Test: release build does not compile Table Viewer code.

## 10. Inspector Panels — Diff Stream

- [ ] 10.1 Implement `DiffStreamWindow` that subscribes to journal commit events (via tick-end hook) and appends commited diffs to a scrollable buffer.
- [ ] 10.2 Each diff entry displays: tick, table name, row ID, column, old value, new value. Color-code insertions (green), updates (yellow), deletions (red).
- [ ] 10.3 Add table filter dropdown: select a table to filter displayed diffs.
- [ ] 10.4 Add tick selection for historical diff viewing from the diff buffer (last N ticks, bounded by `MAX_COMMIT_HISTORY`).
- [ ] 10.5 Add integration test: run a tick with known diffs, verify Diff Stream displays correct entries, verify filter by table works, verify historical view works.

## 11. Inspector Panels — Relation Graph Visualizer

- [ ] 11.1 Implement `RelationGraphWindow` that renders tables as nodes and relations as directed edges using egui canvas or custom painting.
- [ ] 11.2 Node labels display table name. Edge labels display relation name.
- [ ] 11.3 Click a node: highlight node and all incident edges, display table schema in side panel.
- [ ] 11.4 Click an edge: trigger navigation in Table Viewer to the target table, filtered by the relation's referenced rows.
- [ ] 11.5 Add integration test: verify all tables appear as nodes, verify relations appear as edges, verify click navigation.

## 12. Inspector Panels — Snapshot Browser

- [ ] 12.1 Implement `SnapshotBrowserWindow` that lists retained snapshots with tick number and state hash from `ArrowStore`.
- [ ] 12.2 Implement two-snapshot selection: select snapshot A and B (drop-down or click), "Compare" button.
- [ ] 12.3 Implement comparison logic: iterate tables in both snapshots, detect row count differences, detect per-row differences via `WorldView::iter_rows()` comparison, display before/after values.
- [ ] 12.4 Add integration test: create two snapshots with known differences, verify comparison shows correct diffed tables and rows.

## 13. WorldSnapshot Public API Hardening

- [ ] 13.1 Apply `#[deny(missing_docs)]` to `scharnhorst_arrow_store` public API.
- [ ] 13.2 Audit `WorldSnapshot` `pub` methods — verify only `WorldView`-consumable methods remain public. All `RecordBatch`/Arrow accessors are `pub(crate)`.
- [ ] 13.3 Verify `VersionedTable` public surface does not expose Arrow internals to simulation consumers.
- [ ] 13.4 Run `cargo doc --no-deps` and verify no `RecordBatch`, `arrow_array`, or `arrow_schema` types appear in public API docs of `scharnhorst_arrow_store` or `scharnhorst_query`.

## 14. Integration Tests — Read Path Audit

- [ ] 14.1 Test: simulation system cannot compile if it holds `WorldSnapshot` or `RecordBatch` directly.
- [ ] 14.2 Test: `query_engine.snapshot()` returns `WorldView` that cannot reach `RecordBatch` or Arrow methods.
- [ ] 14.3 Test: `validate()` catches a `RulePath` referencing a missing table before first tick.
- [ ] 14.4 Test: `sql()` SELECT returns correct data; `sql()` INSERT is rejected.

## 15. Integration Tests — Telemetry Does Not Affect Determinism

- [ ] 15.1 Run golden replay test with `metrics` feature enabled and disabled. Verify identical tick-by-tick state hashes.
- [ ] 15.2 Verify telemetry events are emitted when metrics enabled (use `tracing-test` or `tracing-subscriber` in test).
- [ ] 15.3 Verify zero telemetry events when metrics disabled.

## 16. Final Cleanup and Documentation

- [ ] 16.1 Run `cargo clippy --all-targets --all-features` and fix all warnings.
- [ ] 16.2 Run `cargo test --all-features` — all tests pass.
- [ ] 16.3 Run `cargo test --release` — verify no inspector or metrics code leaks into release.
- [ ] 16.4 Verify `AGENTS.md` error handling policy: no `unwrap()`, `expect()`, or `panic!()` in new code.