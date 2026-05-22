## ADDED Requirements

### Requirement: Table Viewer Panel
The system SHALL provide a Bevy egui panel that displays registered Arrow tables with their schemas, row counts, and a paginated table viewer. Data is read exclusively through `query-engine` typed APIs or `WorldView` iteration. Available only in debug builds.

#### Scenario: Viewing a table
- **WHEN** the developer opens the Table Viewer panel and selects a table from the dropdown
- **THEN** the panel displays the table's column names and Arrow data types, the total row count, and the first 50 rows
- **AND** the panel header shows the generation tick of the data being viewed
- **AND** all data is read through `query-engine` typed read APIs or `WorldView`

#### Scenario: Paginating through rows
- **WHEN** the developer clicks "Next Page" in the Table Viewer with a page size of 50
- **THEN** rows 51-100 are displayed. For a table with 1000 rows, 20 pages exist.
- **AND** page navigation shows "Page 2 of 20"
- **AND** page size is configurable (25, 50, 100, 200) from a dropdown

#### Scenario: Sorting by column
- **WHEN** the developer clicks a column header
- **THEN** all rows (across all pages) are sorted by that column
- **AND** first click sorts ascending, second click sorts descending
- **AND** a sort direction indicator (arrow) is displayed on the active column header
- **AND** paging through sorted data shows rows in sorted order across pages

#### Scenario: Filtering by column value
- **WHEN** the developer enters a filter string in the Table Viewer's filter input
- **THEN** only rows where at least one column value contains the filter text (substring match) are displayed
- **AND** the displayed row count reflects the filtered set
- **AND** pagination operates within the filtered set

#### Scenario: Table with zero rows
- **WHEN** the developer selects a table that exists in schema but has zero rows
- **THEN** the panel displays the table's schema (column names, types) as usual
- **AND** the row count shows "0 rows"
- **AND** the data area displays a "No rows" message (not an empty table or error)

#### Scenario: Table with columns containing all-null values
- **WHEN** the developer selects a table where a specific column has null values for every row
- **THEN** the column is displayed normally with all cells showing "NULL" or an empty indicator
- **AND** sorting by a null column does not crash (nulls are grouped at top or bottom of sort order)

#### Scenario: Very wide table (50+ columns)
- **WHEN** the developer selects a table with 50 or more columns
- **THEN** the table viewer provides horizontal scrolling
- **AND** the first 5-8 columns remain visible without scrolling (reasonable default)
- **AND** column headers are always visible during horizontal scroll

#### Scenario: Table dropped between ticks while panel is viewing it
- **WHEN** the developer has `temp_events` table selected in Table Viewer at tick 5
- **AND** at tick 6, a migration drops `temp_events`
- **WHEN** the inspector refreshes from the tick 6 `WorldView`
- **THEN** `query_engine.snapshot()` no longer contains `temp_events`
- **AND** the Table Viewer clears its selection and displays "Table no longer exists" message
- **AND** the panel is still usable — it shows the table dropdown reset to "Select a table"

#### Scenario: Stale data indicator
- **WHEN** the inspector holds a `WorldView` at tick 5, but the simulation has advanced to tick 50
- **THEN** the panel header displays "Data from tick 5 (45 ticks behind)" with a visual indicator
- **AND** a "Refresh" button is available to obtain a new `WorldView`
- **AND** the panel does not auto-refresh on every tick (to avoid thrashing during rapid simulation)

### Requirement: Relation Graph Visualizer
The system SHALL provide an interactive graph view of tables and their relations. Tables are displayed as nodes; relations are displayed as directed edges. Clicking a node displays its schema; clicking an edge navigates to the target table.

#### Scenario: Viewing the relation graph
- **WHEN** the developer opens the Relation Graph panel
- **THEN** all registered tables are displayed as labeled nodes
- **AND** all relations from the `RelationGraph` are displayed as directed edges between nodes
- **AND** edge labels show the relation name
- **AND** edges are visually distinct from nodes (different color, arrowhead)

#### Scenario: Clicking a table node
- **WHEN** the developer clicks a table node
- **THEN** the node is highlighted
- **AND** all edges incident to that node are highlighted
- **AND** the side panel displays the table's schema (column names, types) and current row count

#### Scenario: Following a relation edge (navigating to related table)
- **WHEN** the developer clicks a relation edge from table A → table B
- **THEN** the Table Viewer panel (if open) navigates to table B
- **AND** the Table Viewer is filtered to show rows of B that are referenced by selected rows of A (foreign-key jump)
- **AND** if Table Viewer is not open, a message suggests opening it

#### Scenario: Self-referential relations
- **WHEN** a table `actors` has a relation `allies` to itself
- **THEN** the graph displays a single `actors` node with a self-loop edge labeled `allies`
- **AND** clicking the self-loop edge triggers navigation back to `actors`, filtered by the allied-row reference (i.e., it shows the allied actors of the currently selected row)
- **AND** no infinite loop occurs — the edge click is a single navigation step

#### Scenario: Relation graph with 100+ tables
- **WHEN** the schema contains 100 or more registered tables
- **THEN** the graph view provides zoom (scroll wheel) and pan (click-drag) controls
- **AND** nodes are arranged by a layout algorithm (not random placement)
- **AND** labels are readable at default zoom, with detail visible on zoom-in

### Requirement: Diff Inspector (Diff Stream)
The system SHALL provide a live-scrolling panel showing diffs as they are committed at tick boundaries. Diffs SHALL be filterable by table. The diff buffer SHALL be bounded to `MAX_COMMIT_HISTORY` ticks (same as journal history, default 1024).

#### Scenario: Live diff display
- **WHEN** a tick commit completes and the Diff Inspector panel is open
- **THEN** the panel appends newly committed diffs to the display
- **AND** each diff entry shows: tick, table name, row ID, column, old value, and new value
- **AND** insert-type diffs are displayed with a green indicator
- **AND** update-type diffs are displayed with a yellow indicator
- **AND** delete-type diffs are displayed with a red indicator

#### Scenario: Empty diff buffer (fresh session, first tick before any diffs)
- **WHEN** the Diff Inspector panel is opened but no ticks have been committed yet, or a tick committed zero diffs
- **THEN** the panel displays a "No diffs yet" message
- **AND** the panel does not panic or render an empty table with column headers

#### Scenario: Diff buffer bounded to MAX_COMMIT_HISTORY
- **WHEN** the diff buffer reaches 1024 ticks of committed diffs
- **AND** a new tick is committed (tick 1025 with respect to the buffer)
- **THEN** the oldest tick's diff entries are evicted from the buffer
- **AND** the buffer size never exceeds `MAX_COMMIT_HISTORY` worth of diff data
- **AND** historical diff viewing (via tick dropdown) reflects the bounded window — oldest tick shown is `current_tick - MAX_COMMIT_HISTORY + 1`

#### Scenario: Filtering diffs by table
- **WHEN** the developer selects a specific table in the Diff Inspector filter dropdown
- **THEN** only diffs targeting that table are displayed
- **AND** the displayed diff count reflects the filtered count
- **AND** filtering applies to both the live stream view and the historical tick view

#### Scenario: Viewing historical diffs by tick
- **WHEN** the developer selects a past tick from the history dropdown
- **THEN** diffs from that specific tick are displayed
- **AND** the display header shows "Diffs from tick N" with the tick's timestamp
- **AND** new diffs from subsequent ticks do not replace the historical view until the developer switches back to "Live" mode

#### Scenario: Diff Inspector during replay
- **WHEN** the simulation is in replay mode (reconstructing from a journal file)
- **AND** the Diff Inspector panel is open
- **THEN** the panel displays diffs as they are replayed from the journal
- **AND** the display is identical to the original simulation's diff stream (same diffs, same tick numbers, same ordering)
- **AND** replay-mode diffs are not double-submitted (journal replay does not create new diffs; it reconstructs from recorded diffs)

### Requirement: Snapshot Browser
The system SHALL provide a panel for browsing retained snapshots, comparing state between two ticks, and viewing differences.

#### Scenario: Listing retained snapshots
- **WHEN** the developer opens the Snapshot Browser
- **THEN** all retained snapshots are listed chronologically with tick number and state hash
- **AND** the current (latest) snapshot is highlighted
- **AND** the list is scrollable if more snapshots exist than fit on screen

#### Scenario: Snapshot Browser with only one snapshot
- **WHEN** only a single snapshot exists (e.g., tick 0, the initial snapshot)
- **THEN** the snapshot list shows one entry
- **AND** the "Compare" button is disabled (two snapshots are required for comparison)
- **AND** a tooltip on the disabled button reads "Need at least 2 snapshots to compare"

#### Scenario: Comparing two snapshots
- **WHEN** the developer selects snapshot A (tick 10) and snapshot B (tick 20), then clicks "Compare"
- **THEN** the panel iterates tables in both snapshots and displays:
  - Tables that exist in both snapshots but have different row counts
  - Rows that exist in A but not B (deleted), or in B but not A (inserted)
  - Rows that exist in both but have different column values (modified)
- **AND** for each changed row, before/after column values are displayed
- **AND** the diff is computed by comparing `WorldView::iter_rows()` output between the two snapshots

#### Scenario: Comparing a snapshot with itself (same tick)
- **WHEN** the developer selects tick 10 as both snapshot A and snapshot B, and clicks "Compare"
- **THEN** the panel displays "No differences — same snapshot" message
- **AND** no diff computation is performed (short-circuit for identical snapshots)

#### Scenario: Viewing state hash chain
- **WHEN** the developer switches to the "Hash Chain" view in the Snapshot Browser
- **THEN** a chronological list of tick numbers and their state hashes is displayed
- **AND** any hash mismatch between consecutive ticks' incremental hashes is highlighted
- **AND** clicking a tick entry navigates to that snapshot's detailed view

#### Scenario: Snapshot Browser during save/load
- **WHEN** a save operation is writing a snapshot to disk, or a load operation is reconstructing snapshots
- **AND** the Snapshot Browser panel is open
- **THEN** the panel gracefully handles the transient state where `ArrowStore`'s snapshot list is changing
- **AND** if the currently viewed snapshot is removed during load (replaced by reconstructed snapshots), the panel resets to the latest snapshot
- **AND** the panel does not panic if a snapshot it holds a `WorldView` for is no longer in the active `ArrowStore` list (the `WorldView`'s `Arc<WorldSnapshot>` keeps the data valid)

### Requirement: Inspector Read-Only Guarantee
All inspector panels SHALL read world state exclusively through `query-engine` typed APIs or `query_engine.snapshot()`. Inspector panels MUST NOT hold mutable references to Authority tables, submit diffs, hold `Arc<ArrowStore>`, `CommitStore`, `InitStore`, or any mutation capability token. Panel interactions (clicking, selecting, typing in filter fields, paginating, sorting) SHALL NOT produce Commands or journal submissions.

#### Scenario: Inspector cannot mutate state
- **WHEN** any inspector panel renders or responds to user interaction
- **THEN** no Authority table, journal diff queue, schema registry, or content manifest is mutated
- **AND** all data access is through read-only `query-engine` APIs
- **AND** no `Command`, `Diff`, or `DiffBatch` is created by any inspector code path

#### Scenario: Inspector cannot hold mutation capability
- **WHEN** the inspector system initializes and requests data handles from the Bevy world
- **THEN** it receives `QueryEngine` handles and `WorldView` references
- **AND** it does NOT receive `Arc<ArrowStore>`, `CommitStore`, `InitStore`, `JournalSubmitToken`, or `CommitToken`
- **AND** the type system prevents inspector code from calling `apply_diffs`, `create_table`, `register_type`, or any mutation API

### Requirement: Inspector State Ephemerality
All inspector panel state SHALL be Ephemeral. This includes: selected table, current page, sort column and direction, filter text, scroll position, diff buffer contents, relation graph layout, Snapshot Browser comparison selections, and window positions.

Inspector state SHALL NOT be persisted in save files, replay files, journal records, or state hashes.

Closing a panel and reopening it SHALL return the panel to its default state (no selections, empty filters, first page). There is no inspector session persistence.

#### Scenario: Panel reopening resets state
- **WHEN** the developer closes the Table Viewer panel (it has `actor_state` selected, page 3, sorted by `treasury` descending, filter "Prussia")
- **AND** the developer reopens the Table Viewer panel
- **THEN** the panel shows "Select a table" with no table selected, page 1, no sort, no filter
- **AND** previous selections are not recovered

#### Scenario: Window state not in state hash
- **WHEN** the Table Viewer panel is open with a filter applied during a tick
- **AND** the journal commits that tick
- **THEN** the tick's state hash is computed solely from Authority table contents
- **AND** the presence or state of inspector panels does not affect the hash
- **AND** replaying the tick produces the same hash regardless of whether inspector panels were open

### Requirement: Inspector Debug-Build Gating
All inspector panels SHALL be conditionally compiled under `#[cfg(debug_assertions)]`. The `InspectorPlugin` and all panel modules SHALL be absent from release builds.

#### Scenario: Release build has no inspector
- **WHEN** the project is compiled with `--release`
- **THEN** no inspector panel code is included in the binary
- **AND** no `bevy_egui` dependency is linked (guarded by `#[cfg(debug_assertions)]` in `Cargo.toml`)
- **AND** `InspectorPlugin` does not appear in the Bevy app's plugin list

#### Scenario: InspectorPlugin double-registration
- **WHEN** a debug build adds `InspectorPlugin` to the Bevy app twice (e.g., in both a test harness and a helper function)
- **THEN** Bevy's plugin deduplication prevents double initialization
- **AND** the inspector operates normally with a single set of panel state resources

#### Scenario: Inspector during replay
- **WHEN** the simulation is in replay mode and `InspectorPlugin` is registered
- **THEN** all inspector panels work identically to live simulation mode
- **AND** table data is read from replayed snapshots via `query_engine`
- **AND** Diff Stream shows diffs as they are replayed
- **AND** Snapshot Browser shows snapshots reconstructed from the replay journal

### Requirement: Inspector Error Isolation
Inspector panel failures SHALL NOT propagate to the simulation. If a panel encounters an error (missing table, type mismatch, query failure), the error SHALL be displayed in the panel's UI area and logged as a `tracing::warn!` event. The simulation continues normally.

#### Scenario: Table Viewer fails to read table — simulation continues
- **WHEN** the Table Viewer encounters an error reading a table (e.g., the table was dropped and the panel hasn't refreshed yet)
- **THEN** the error is displayed in the panel's body area with a descriptive message
- **AND** a `tracing::warn!("inspector_read_error", error = "...")` event is emitted
- **AND** the simulation tick continues unaffected — no panic, no error propagation to scheduler

### ADDED Invariants

#### Invariant: Inspector Read-Only (I-INSPECTOR-READ-ONLY)
Inspector panels SHALL read world state exclusively through `query-engine` APIs or `WorldView`. Panels SHALL NOT hold `Arc<ArrowStore>`, `CommitStore`, `InitStore`, or any mutation capability token. Panel interactions SHALL NOT produce Commands, Diffs, or journal submissions. This invariant extends I-JRN-SINGLE-WRITE to the UI layer.

#### Invariant: Inspector Ephemeral State (I-INSPECTOR-EPHEMERAL)
All inspector state — selection, pagination, filter text, scroll position, diff buffer, relation graph layout, Snapshot Browser selections, window positions — is Ephemeral. Inspector state SHALL NOT be persisted in save files, replay files, or state hashes. Closing and reopening a panel returns it to default state. This invariant extends I-BB-STATE-TIER (which covers Bevy ECS components) to cover egui UI state.