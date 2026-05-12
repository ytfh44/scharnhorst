**Crate**: `scharnhorst_bevy`

## ADDED Requirements

### Requirement: Entity Materialization
The system SHALL materialize Bevy entities only for Arrow rows that are currently relevant to the view (e.g., visible on map, selected in UI).

#### Scenario: Province visibility
- **WHEN** a province entity enters the camera frustum
- **THEN** the bridge creates a Bevy entity with a `ViewOf` component linking to the Arrow row.

### Requirement: View Model Synchronization
The system MUST synchronize state from the `WorldSnapshot` to Bevy components using a dedicated sync phase.

#### Scenario: Color update
- **WHEN** the owner of a province changes in the Arrow store
- **THEN** the bridge updates the `MapColor` component of the corresponding Bevy entity.

### Requirement: Command Buffering
The bridge SHALL buffer player interactions into a command queue. These commands are consumed by the `sim-scheduler` at the start of each tick and submitted to the `journal-system` as `Command` objects.

#### Scenario: Clicking a button
- **WHEN** a player clicks "Declare War" in the UI
- **THEN** the bridge pushes a `DeclareWar` command into the `InputCommandBuffer`.

## MODIFIED Requirements

### (Compatibility) Requirement: Tick-Aligned Command Consumption (↔ sim-scheduler, ↔ journal-system)
The `InputCommandBuffer` SHALL accumulate player commands per frame and expose them to the `sim-scheduler` once per tick. Commands arriving mid-tick are buffered and delivered at the next tick boundary. The scheduler is the sole consumer of the buffer.

#### Scenario: Multiple clicks within one tick
- **WHEN** a player clicks "Move" three times between two tick boundaries
- **THEN** the bridge stores all three commands; the scheduler pulls all three at tick start and submits them to the journal.

### (Compatibility) Requirement: Read-Only Snapshot Access (↔ query-engine, ↔ sim-scheduler)

The `bevy-bridge` SHALL obtain an immutable `WorldSnapshot` reference **via the `query-engine`** to drive ViewModel synchronization. The bridge:
- MUST NOT hold mutable references to Arrow tables.
- MUST NOT bypass the `journal-system` to write diffs.
- MUST NOT reference Arrow `RecordBatch` or `Table` objects directly; all reads MUST go through `query-engine`.
- MUST NOT query the `schema-registry`'s RelationGraph directly (accessed via `query-engine`).
- SHALL refresh its snapshot reference **after** receiving the refresh signal from `sim-scheduler` (triggered by `journal.commit()` at tick boundary), discarding the prior snapshot.

#### Component Lifetime Constraint:
All Bevy Component types defined in `scharnhorst_bevy` SHALL satisfy `Component: 'static`. Fields containing string data SHALL use `String` (owned heap-allocated) rather than `&'static str`. The `ViewOf` component SHALL store:
- `row_id: RowId` (u64 wrapper — `Copy`)
- `table_name: String` (owned — NOT `&'static str`)
- `generation: u64`

Using `&'static str` for component fields is FORBIDDEN because it either requires `Box::leak` (memory leak) or a global string interner (complexity). `String` is the correct choice for Bevy Component fields that hold textual data.

#### REFRESH_SIGNAL Protocol (see `sim-scheduler` DC-10)

The `bevy-bridge` registers a refresh handler with the `sim-scheduler`. The lifecycle is:

```
T+0 (end of PostTick):
1. sim-scheduler broadcasts REFRESH_SIGNAL
2. bevy-bridge receives signal

T+1 (before next frame sync):
3. Bridge discards old snapshot reference (generation N)
4. Bridge obtains new snapshot reference (generation N+1) via query-engine.get_snapshot()
5. Entity synchronization proceeds with fresh snapshot
```

#### Refresh handler error handling (CLARIFIED):
The refresh handler SHALL handle `Mutex`/`RwLock` poisoning as a **recoverable error**, not a panic. Panicking via `.expect()` or `.unwrap()` on lock acquisition is FORBIDDEN. Instead, the handler SHALL use `.map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))` to propagate the error through the scheduler's refresh signal protocol.

This is consistent with:
- DC-10's requirement that the scheduler SHALL NOT panic on invalid state
- The `sync.rs` module's existing lock poisoning pattern
- The error-preserving refresh signal semantics in `sim-scheduler`

#### Scenario: Lock poisoning during refresh
- **WHEN** a previous operation panicked while holding the refresh handler's `Mutex`
- **THEN** the handler SHALL return `Err(BevyBridgeError::LockPoisoned(...))` to the scheduler
- **AND** the scheduler SHALL propagate the error through `SchedulerResult` (not panic)
- **AND** the application SHALL decide recovery policy (e.g., restart tick, shutdown gracefully)

#### Scenario: Bridge reads after commit
- **WHEN** the `sim-scheduler` triggers `journal.commit()` at tick boundary and signals consumers to refresh
- **THEN** the bridge receives REFRESH_SIGNAL, discards old snapshot reference
- **AND** the bridge obtains the new `WorldSnapshot` (generation N+1) via `query-engine`
- **THEN** the bridge begins synchronizing entities from the fresh snapshot in the next frame.

### (Compatibility) Requirement: Player Commands Only (↔ journal-system)
The Bevy bridge SHALL ONLY buffer **player-originated** commands. AI and internal simulation commands bypass the bridge entirely and write directly to the `journal-system`. This ensures the bridge remains a UI-layer concern and does not become a bottleneck for simulation systems.

#### Scenario: AI declares war
- **WHEN** an AI system decides to declare war
- **THEN** it calls `journal_system.submit(Command::DeclareWar { ... })` directly, without going through the bridge buffer.
