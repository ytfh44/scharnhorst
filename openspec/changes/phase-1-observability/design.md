## Context

Phase 0 established capability tokens and state tiering. The `QueryEngine` spec already asserts it is "the sole simulation read interface," but `WorldSnapshot` is still public API with `table()` and arrow table accessors. Several consumers in `scharnhorst_rules`, `scharnhorst_bevy`, and `scharnhorst_save` hold snapshot references directly. The ROADMAP's Phase 1 requires making `QueryEngine` the **exclusive** read interface — not just designated, but enforced by API surface.

Simultaneously, the engine produces zero runtime telemetry. Debugging a deterministic simulation without visibility into tick timing, query patterns, or diff volumes is blindfold work. This design adds instrumentation hooks and inspector tooling that do not affect determinism or add overhead in production.

Stakeholders:
- Simulation system authors: need a narrow, typed read API with no foot-guns.
- Debug tooling developers: need SQL console and egui panels for state inspection.
- Replay/multiplayer code: must not be affected — telemetry must not alter determinism.

## Goals / Non-Goals

**Goals:**
- Make `QueryEngine` the exclusive read interface. No consumer holds direct `WorldSnapshot`, raw `RecordBatch`, or `ArrowStore` internals.
- Add `QueryEngine::validate()` for early schema-path checking at load time.
- Add `QueryEngine::sql()` for ad-hoc DataFusion SELECT queries (debug builds).
- Build inspector panels in `scharnhorst_bevy` (debug builds only): table viewer, diff stream, relation graph visualizer, snapshot browser.
- Add zero-cost instrumentation to scheduler, query engine, and journal via `tracing`.
- Inspector panels read through `query-engine`; never write Authority state.
- Production builds compile out all inspector panels and all telemetry.

**Non-Goals:**
- Implement the full Phase 1 inspector suite (rule debugger, modifier chain inspector, performance dashboard are Phase 2/5).
- Replace DataFusion or the existing typed access model.
- Implement mid-simulation schema changes.
- Add a network-observability layer (multiplayer telemetry is Phase 4).
- Persist telemetry data — tracing export is push-based, no internal storage.

## Decisions

### Decision 1: Snapshot Handle Narrowing — `WorldView` Instead of Raw `WorldSnapshot`

`QueryEngine::snapshot()` will return a `WorldView` type that:
- Borrows the `Arc<WorldSnapshot>` internally.
- Exposes only read-only table iteration (for materialization/tooling).
- Does NOT expose `arrow_table()`, `RecordBatch`, or `Schema` methods.
- Cannot be cloned into a mutable path.

Alternative considered: remove `snapshot()` entirely and force all consumers through `batch_reader` or `read()`. Rejected because Bevy materialization and inspector tooling need to iterate tables for row-at-a-time entity sync, and forcing them through batch APIs would require pre-declaring all tables and columns.

Rationale: `WorldView` provides a narrow, auditable surface for the small set of consumers that genuinely need table-level iteration (entity materialization, inspector panels), while blocking the dangerous `arrow_table()` access that Phase 0's I-AS-AUTHORITY-ONLY invariant warns against.

### Decision 2: Inspector Panels Live in `scharnhorst_bevy`, Gated Behind `cfg(debug_assertions)`

Inspector panels (Table Viewer, Diff Stream, Relation Graph, Snapshot Browser) are implemented as Bevy egui systems. Each panel:
- Exists only in debug builds (`#[cfg(debug_assertions)]`).
- Reads through `query-engine` exclusively.
- Exposes no mutation APIs.

The panels are added as a Bevy plugin: `InspectorPlugin` that registers egui window systems. Splitting panels into separate modules (`table_viewer.rs`, `diff_stream.rs`, `relation_graph.rs`, `snapshot_browser.rs`) avoids monolithic UI code.

Alternative considered: put inspector in a separate `scharnhorst_inspector` crate. Rejected because it would add a crate solely for debug tooling with a one-line feature gate — the same isolation is achieved via `#[cfg(debug_assertions)]` without crate overhead.

### Decision 3: Telemetry Uses `tracing` Macros with Compile-Time Gating

All instrumentation uses `tracing` spans and events:
- `tracing::info_span!` with `tick`, `phase`, `system_name` fields for scheduler timing.
- `tracing::debug!` events for query latency, cache hits, diff counts.
- `tracing::warn!` for anomalies (e.g., zero-length diff batches).

A `metrics` feature flag on each crate gates instrumentation. When disabled (default for release), all `tracing` calls are `cfg`-compiled out. No runtime branching.

The scheduler adds a `metrics` field to `Scheduler` (gated behind `#[cfg(feature = "metrics")]`) that records per-phase `Instant` timestamps and diff counts. These are emitted as tracing events at tick end.

Alternative considered: custom metrics registry with internal aggregation. Rejected because `tracing` is already a dependency, and the `tracing-subscriber` ecosystem handles export to console, file, or OTLP without additional code.

### Decision 4: SQL Console Uses Existing `SqlExecutionContext`

`QueryEngine::sql()` wraps the existing `SqlExecutionContext` with a public method:
```rust
pub fn sql(&self, sql: &str) -> QueryResult<RecordBatch>
```

The method:
- Acquires the read lock on `sql_context`.
- Runs the query against the current `view_cache` tables via DataFusion.
- Only allows SELECT statements. INSERT/UPDATE/DELETE are rejected (not routed to `DebugWriteJournal` — that path is separate).

Alternative considered: expose a DataFusion `SessionContext` directly. Rejected because it leaks internal state and bypasses the `view_cache` ingestion ordering guarantees (I-QE-TICK-ORDERING).

### Decision 5: Read Path Audit Is Sequential Per-Crate

The audit migrates one crate at a time:
1. `scharnhorst_rules` — evaluator already uses `lookup_row`; audit for any remaining `WorldSnapshot` access.
2. `scharnhorst_bevy` — `SyncState` and `ViewModel` currently hold `Arc<WorldSnapshot>`. Migrate to `WorldView` from `query-engine`.
3. `scharnhorst_save` — save/load reconstruction accesses `ArrowStore` during initialization (already allowed via `InitToken`). Audit replay path.
4. `scharnhorst_arrow_store` — remove public `table()` accessor from `WorldSnapshot`. Add `WorldView` type for mediated access.

Each migration can be verified by integration tests that assert no cross-boundary access.

Rationale: sequential per-crate migration allows each PR to be tested independently. The `WorldSnapshot -> WorldView` transition is the only cross-crate coupling point.

### Decision 6: `QueryEngine::validate()` Uses `RulePath` Registry

At the end of content loading (Phase 4 of the six-phase lifecycle), `validate()`:
- Iterates all registered `RulePath` entries (from `name_resolution` in `scharnhorst_content`).
- Checks each path's table and column against `SchemaRegistry`.
- Checks each relation chain against `RelationGraph`.
- Returns a `Vec<ValidationError>` with table, column, and path information.

Fails early — before first tick — with actionable error messages. Does not require any new type registrations.

## Risks / Trade-offs

- **`WorldView` API design risk**: Too narrow and inspector panels can't build useful views; too wide and it becomes a backdoor. Mitigation: start with `WorldView` exposing only `table_names()`, `row_count()`, `column_names()`, `column_type()`, `iter_rows()` with typed getters. Expand conservatively based on inspector needs.
- **Audit may reveal hidden dependencies on `RecordBatch` internals**: Some test harness or save code may access Arrow arrays directly. Mitigation: use the same migration approach — wrap in helper types, not raw Arrow access.
- **DataFusion SELECT overhead**: Running SQL against the snapshot for every inspector frame would be slow. Mitigation: inspector panels use typed read APIs (`batch_reader`, `column_view`) for their primary data; `sql()` is for ad-hoc developer queries only, not for panel rendering loops.
- **Tracing export to file may interfere with deterministic replay**: Tracing subscribers that write to disk on the scheduler thread could perturb timing. Mitigation: use `tracing_subscriber::fmt` with non-blocking writer or spawn subscriber on a separate thread. For replay mode, disable tracing entirely.
- **`bevy_egui` adds a new dependency**: Mitigation: gated behind `#[cfg(debug_assertions)]`, so production builds have no dependency on `bevy_egui`.

## Migration Plan

1. Add `WorldView` type to `scharnhorst_arrow_store` without changing existing consumers.
2. Audit `scharnhorst_rules` for any direct `WorldSnapshot` access. Migrate to `query-engine` APIs.
3. Migrate `scharnhorst_bevy` from `Arc<WorldSnapshot>` to `WorldView` from `query-engine`. Replace `ViewModel::snapshot` with `ViewModel::world_view`.
4. Remove public `WorldSnapshot::table()` and `WorldSnapshot::arrow_table()` accessors. Make them `pub(crate)`.
5. Add `QueryEngine::validate()` and `QueryEngine::sql()`.
6. Instrument scheduler, query engine, journal with `tracing` spans.
7. Build inspector panels behind `#[cfg(debug_assertions)]` in `scharnhorst_bevy`.
8. Add integration tests: read path audit (no consumer holds `WorldSnapshot` directly), `validate()` catches schema errors, `sql()` returns correct results, inspector panels render without panic.
9. Remove or deprecate any remaining public Arrow-access surfaces.

Rollback: each step is a source-level change. Data format is unchanged — Authority snapshots are not affected.

## Phase 1 Invariants (Additive)

Phase 1 does not weaken any Phase 0 invariant. The following invariants are additive — they constrain the new surfaces introduced by this phase.

### I-WV-IMMUTABLE — WorldView Frozen Snapshot

WorldView is a frozen point-in-time snapshot. Its data SHALL NOT change after construction. A `WorldView` obtained at tick T SHALL always return data from tick T, even if the consumer holds it across subsequent ticks. This prevents consumers from observing phantom diffs or partially-applied state during multi-table ingestion.

**Interaction with I-AS-AUTHORITY-ONLY** (Phase 0): WorldView exposes only Authority tables from the snapshot. Derived and Ephemeral tables are not in ArrowStore and thus not visible through WorldView.

### I-WV-ARC-BACKED — WorldView Survives Truncation

`WorldView` SHALL internally hold `Arc<WorldSnapshot>`. The `WorldView` SHALL remain valid after `ArrowStore` truncates old snapshots via `truncate_before()`. The `Arc` reference count keeps the `WorldSnapshot` alive until the last `WorldView` handle drops it. Consumers do not implement their own lifetime management.

### I-SQL-FRESH-SESSION — No DataFusion State Leak

`sql()` SHALL create a fresh `SessionContext` per invocation. No session state — temporary tables, session variables, cached plans, UDFs — SHALL persist between calls. The naive approach of reusing a shared `SessionContext` would allow `CREATE TABLE foo AS SELECT ...` in one call to make `foo` visible in the next call, violating I-QE-READ-ONLY (Phase 0). The fresh-session pattern prevents this.

### I-SQL-DEBUG-CONSISTENCY — Best-Effort Consistency

`sql()` and inspector panel reads MAY observe partially-ingested snapshots during the microsecond window of multi-table ingestion in `journal.commit()`. When table A has been ingested at tick N but table B is still at tick N-1, a cross-table `SELECT` from the UI thread returns mixed-tick data. This is acceptable because:

1. Simulation systems run on the scheduler thread sequentially — they never see partial state.
2. The ingestion window is bounded by sequential commit duration (microseconds).
3. Debug tooling operates on best-effort consistency, not guaranteed ACID isolation.

### I-TELEMETRY-NON-BLOCKING — No Subscriber Blocking

Telemetry SHALL NOT block the simulation thread for unbounded durations. `tracing` subscribers SHALL use non-blocking writers. If a subscriber's buffer is full, events SHALL be dropped silently rather than blocking `Instant::now()` or `tracing::info!()`. Replay mode SHALL disable all telemetry.

**Naive approach avoidance**: Using a synchronous file writer as the subscriber target would cause the simulation thread to block on disk I/O during `tracing::info!()` calls. A slow disk (network mount, spinning rust, antivirus scan) could delay tick execution by tens of milliseconds, artificially inflating timing metrics and potentially causing the subscriber to become a bottleneck. Non-blocking writer with event-dropping prevents this.

### I-TELEMETRY-DETERMINISM — Metrics Do Not Affect State

The presence or absence of telemetry SHALL NOT affect state hashes, diff computation, RNG streams, or any deterministic computation. Telemetry data SHALL be excluded from save files, replay files, journal records, and hash inputs. A simulation run with `metrics` enabled SHALL produce byte-identical `CommitRecord` data to the same run with `metrics` disabled.

**Naive approach avoidance**: Storing timing data in `CommitRecord` (e.g., adding `commit_duration_micros: u64` as a field) would cause replay hashes to diverge from original hashes because the timing field would differ between original run and replay. The invariant mandates that `CommitRecord` remains unchanged; telemetry is push-side only.

### I-INSPECTOR-READ-ONLY — UI Cannot Mutate

Inspector panels SHALL read world state exclusively through `query-engine` APIs or `WorldView`. Panels SHALL NOT hold `Arc<ArrowStore>`, `CommitStore`, `InitStore`, or any mutation capability token. Panel interactions SHALL NOT produce Commands, Diffs, or journal submissions.

**Interaction with I-JRN-SINGLE-WRITE** (Phase 0): This invariant extends the single-write-entry-point constraint to the UI layer. Clicking a table cell in the Table Viewer, selecting a relation edge in the Relation Graph, or browsing diffs in the Diff Stream SHALL NOT generate any journal-write activity.

**Naive approach avoidance**: Exposing `CommitToken` or `JournalSubmitToken` to the Bevy world as a resource would allow any `Res<CommitToken>` system (including inspector systems added by third-party plugins) to inject diffs. The inspector receives only `QueryEngine` handles and `WorldView` references — no token types.

### I-INSPECTOR-EPHEMERAL — UI State Not Persisted

All inspector state — table selection, page number, sort direction, filter text, scroll position, diff buffer contents, relation graph layout, Snapshot Browser selections, window positions — is Ephemeral. Inspector state SHALL NOT be persisted in save files, replay files, journal records, or state hashes. Closing and reopening a panel returns it to default state.

**Interaction with I-BB-STATE-TIER** (Phase 0): This invariant extends the Bevy-component tiering (Authority/Derived/Ephemeral) to egui UI state. Inspector state is Ephemeral by nature — it has no reconstruction formula from Authority data and is not meaningful outside the current debugging session.

### I-SCHED-SPAN-DROP-CLOSE — Timing Guards Close on Panic

All scheduler timing spans SHALL be entered via guard types (`DurationGuard`) with `Drop` implementations that close the span. If a phase or system panics, `Drop` SHALL close the span during stack unwinding.

**Naive approach avoidance**: Manually entering a `tracing::span!()` and forgetting to `.exit()` is a leak — the span accumulates in tracing's subscriber state. If a panic occurs between `.enter()` and `.exit()`, the span is leaked. The guard pattern (`Drop` closes span) prevents leaks regardless of panic or early return.

## Naive Implementation Avoidances (Detail)

This section catalogs the worst behaviors of naive implementations that the design decisions prevent.

| Naive Approach | Worst Behavior | Prevented By |
|---|---|---|
| WorldView holds `&WorldSnapshot` (borrow) | Use-after-free after snapshot truncation | I-WV-ARC-BACKED: WorldView stores `Arc<WorldSnapshot>` |
| `sql()` reuses shared `SessionContext` | `CREATE TABLE` in one call leaks to next call, violating read-only | I-SQL-FRESH-SESSION: Fresh `SessionContext` per invocation |
| `validate()` skips RelationGraph check | RulePath passes table/column but relation doesn't exist — fails at first evaluation, not at validate() | validate() also iterates RelationGraph edges |
| Diff Stream buffer unbounded | 100K ticks × 1000 diffs/tick = 1.28 GB, OOM | Buffer bounded to `MAX_COMMIT_HISTORY` (1024 ticks) |
| Synchronous `tracing` subscriber writer | Slow disk blocks simulation thread for 50ms per event | I-TELEMETRY-NON-BLOCKING: non-blocking writer + drop on full |
| Inspector receives `CommitToken` as Bevy resource | Any inspector system (third-party plugin) can inject diffs | I-INSPECTOR-READ-ONLY: Inspector only receives `QueryEngine` + `WorldView` |
| `CommitRecord` stores timing metadata | Replay hash diverges from original because timing differs | I-TELEMETRY-DETERMINISM: `CommitRecord` unchanged, telemetry is push-side |
| Manual `span.enter()` without guard | Panic between enter/exit leaks span forever | I-SCHED-SPAN-DROP-CLOSE: Guard types with `Drop` impl |
| Access pattern HashMap unbounded | Not actually dangerous — bounded by tables × columns (~2000 entries) | N/A — acceptable without explicit bound |
| `sql()` allows semicolon-separated multi-statement | `SELECT 1; DROP TABLE actor_state` — second statement mutates | Pre-execution keyword scan rejects non-SELECT, multi-statement rejected |

## Open Questions

- Should `WorldView` support iteration by `RowId` range, or only full-table iteration? Preferred: full-table iteration for first pass; row-range if inspector pagination proves too slow without it.
- Should `sql()` be available in release builds behind a feature flag? Preferred: debug-only for Phase 1. Release SQL can be reconsidered in Phase 5 when the content editor is built.
- Should telemetry span names be stable (for downstream tooling contracts) or best-effort? Preferred: stable — use `const` span names with defined field keys (`tick`, `phase`, `system_name`, `table`, `column`).