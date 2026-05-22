## ADDED Requirements

### Requirement: Inspector Panel System
The `bevy-bridge` SHALL provide an `InspectorPlugin` that registers Bevy egui panels for world state inspection. All panels SHALL be compiled only in debug builds (`#[cfg(debug_assertions)]`). Panels include: Table Viewer, Diff Stream, Relation Graph Visualizer, and Snapshot Browser.

#### Scenario: InspectorPlugin registers panels
- **WHEN** the Bevy app adds `InspectorPlugin` in a debug build
- **THEN** egui window systems are registered for the Table Viewer, Diff Stream, Relation Graph, and Snapshot Browser
- **AND** all panels obtain data exclusively through `query-engine`

#### Scenario: InspectorPlugin absent in release
- **WHEN** the project is compiled with `--release`
- **THEN** `InspectorPlugin` is not compiled
- **AND** no inspector panels appear in the Bevy app

#### Scenario: InspectorPlugin double-registration
- **WHEN** a debug build adds `InspectorPlugin` to the Bevy app twice
- **THEN** Bevy's plugin deduplication prevents double initialization
- **AND** the inspector operates normally with a single set of panel state resources

#### Scenario: Inspector during replay
- **WHEN** the simulation is in replay mode and `InspectorPlugin` is registered
- **THEN** all inspector panels work identically to live simulation mode
- **AND** panel data is read from replayed snapshots via `query-engine`
- **AND** Diff Stream shows diffs as they are replayed from the journal

### Requirement: Inspector Read Path
All inspector panels SHALL obtain world data exclusively through `query-engine` APIs. Panels SHALL NOT hold `Arc<ArrowStore>`, `CommitStore`, `InitStore`, raw `RecordBatch`, or `WorldSnapshot` references.

#### Scenario: Inspector data access
- **WHEN** any inspector panel needs world data
- **THEN** it calls `query-engine` typed read APIs or `query_engine.snapshot()` for `WorldView`
- **AND** it does not access `ArrowStore` or raw Arrow internals directly

#### Scenario: Inspector cannot hold mutation capability
- **WHEN** the inspector system initializes and requests data handles from the Bevy world
- **THEN** it receives `QueryEngine` handles and `WorldView` references
- **AND** it does NOT receive `Arc<ArrowStore>`, `CommitStore`, `InitStore`, `JournalSubmitToken`, or `CommitToken`
- **AND** the type system prevents inspector code from calling any mutation API

### Requirement: Inspector Error Isolation
Inspector panel failures SHALL NOT propagate to the simulation. If a panel encounters an error, the error SHALL be displayed in the panel's UI area and logged as a `tracing::warn!` event. The simulation SHALL continue normally.

#### Scenario: Table Viewer fails to read table — simulation continues
- **WHEN** the Table Viewer encounters an error reading a table
- **THEN** the error is displayed in the panel's body area with a descriptive message
- **AND** a `tracing::warn!` event is emitted
- **AND** the simulation tick continues unaffected — no panic, no error propagation to scheduler

### Requirement: Inspector State Ephemerality
All inspector panel state SHALL be Ephemeral: selected table, current page, sort column and direction, filter text, scroll position, diff buffer contents, relation graph layout, Snapshot Browser comparison selections, window positions. Inspector state SHALL NOT be persisted in save files, replay files, journal records, or state hashes.

#### Scenario: Panel reopening resets state
- **WHEN** the developer closes and reopens the Table Viewer panel
- **THEN** the panel shows "Select a table" with no table selected, page 1, no sort, no filter
- **AND** previous selections are not recovered

#### Scenario: Inspector state not in state hash
- **WHEN** the Table Viewer panel is open during a tick
- **AND** the journal commits that tick
- **THEN** the tick's state hash is computed solely from Authority table contents
- **AND** inspector panel state does not affect the hash

## MODIFIED Requirements

### Requirement: Read-Only Snapshot Access (query-engine, sim-scheduler)
The `bevy-bridge` SHALL obtain committed world data via `query-engine` to drive entity materialization and view model synchronization. The bridge:

- MUST NOT hold mutable references to Arrow tables.
- MUST NOT bypass `journal-system` to write diffs.
- MUST NOT reference raw Arrow `RecordBatch` or table internals directly.
- MUST NOT query the schema-registry `RelationGraph` directly.
- MUST NOT hold a raw `WorldSnapshot` reference — SHALL use `WorldView` from `query_engine.snapshot()`.
- SHALL refresh read-only committed data after receiving `REFRESH_SIGNAL`.
- SHALL treat Bevy ECS components, view models, hover state, animation state, and frame-local caches as Derived or Ephemeral state, not Authority state.

**REFRESH_SIGNAL protocol:**

```
T+0 end of PostTick:
1. sim-scheduler broadcasts REFRESH_SIGNAL.
2. bevy-bridge receives signal.

T+1 before next frame sync:
3. bridge discards old committed data handles.
4. bridge obtains fresh `WorldView` via query-engine.
5. entity synchronization proceeds with generation N+1 data.
```

**Refresh handler error handling:**
The refresh handler SHALL handle `Mutex`/`RwLock` poisoning as a **recoverable error**, not a panic. Panicking via `.expect()` or `.unwrap()` on lock acquisition is FORBIDDEN. Instead, the handler SHALL use `.map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))` to propagate the error through the scheduler's refresh signal protocol.

**Component lifetime constraint:**
All Bevy Component types defined in `scharnhorst_bevy` SHALL satisfy `Component: 'static`. Fields containing string data SHALL use owned `String` rather than borrowed `&'static str`. The `ViewOf` component SHALL store:
- `row_id: RowId` (u64 wrapper -- `Copy`)
- `table_name: String` (owned -- NOT `&'static str`)
- `generation: u64`

#### Scenario: Bridge reads after commit
- **WHEN** `sim-scheduler` commits a tick and signals consumers
- **THEN** the bridge discards old `WorldView` handles
- **AND** synchronizes entities from fresh query-engine data via `WorldView`
- **AND** cannot mutate Authority state through the read handle

#### Scenario: Bridge view state excluded from saves
- **WHEN** a province entity has hover and animation components
- **THEN** those components are treated as Ephemeral Bevy state
- **AND** they are not persisted as Authority state

#### Scenario: Lock poisoning during refresh
- **WHEN** a previous operation panicked while holding the refresh handler's `Mutex`
- **THEN** the handler SHALL return `Err(BevyBridgeError::LockPoisoned(...))` to the scheduler
- **AND** the scheduler SHALL propagate the error through `SchedulerResult` (not panic)
- **AND** the application SHALL decide recovery policy (e.g., restart tick, shutdown gracefully)

#### Scenario: Bridge holds WorldView not WorldSnapshot
- **WHEN** the bridge materializes entities for the current generation
- **THEN** it accesses table data through `WorldView::iter_rows()` from `query_engine.snapshot()`
- **AND** it does not hold a raw `WorldSnapshot` reference
- **AND** it cannot access `RecordBatch` or Arrow internals through the handle

#### Scenario: WorldView shared between sync and inspector
- **WHEN** both the entity materialization system and the Inspector panels hold `WorldView` handles from the same `query_engine.snapshot()` call
- **THEN** each holds an independent `WorldView` (each with its own `Arc<WorldSnapshot>` increment)
- **AND** they do not interfere with each other — both see consistent, tick-identical data
- **AND** dropping one does not invalidate the other

### ADDED Invariants

#### Invariant: Inspector Read-Only (I-INSPECTOR-READ-ONLY)
Inspector panels SHALL read world state exclusively through `query-engine` APIs or `WorldView`. Panels SHALL NOT hold `Arc<ArrowStore>`, `CommitStore`, `InitStore`, or any mutation capability token. Panel interactions SHALL NOT produce Commands, Diffs, or journal submissions. This invariant extends I-JRN-SINGLE-WRITE to the UI layer.

#### Invariant: Inspector Ephemeral State (I-INSPECTOR-EPHEMERAL)
All inspector state — selection, pagination, filter text, scroll position, diff buffer, relation graph layout, Snapshot Browser selections, window positions — is Ephemeral. Inspector state SHALL NOT be persisted in save files, replay files, or state hashes. This invariant extends I-BB-STATE-TIER (which covers Bevy ECS components) to cover egui UI state.