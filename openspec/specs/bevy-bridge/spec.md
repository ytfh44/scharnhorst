## Requirements

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

### Requirement: Tick-Aligned Command Consumption (sim-scheduler, journal-system)
The `InputCommandBuffer` SHALL accumulate player commands per frame and expose them to the `sim-scheduler` once per tick. Commands arriving mid-tick are buffered and delivered at the next tick boundary. The scheduler is the sole consumer of the buffer.

#### Scenario: Multiple clicks within one tick
- **WHEN** a player clicks "Move" three times between two tick boundaries
- **THEN** the bridge stores all three commands; the scheduler pulls all three at tick start and submits them to the journal.

### Requirement: Read-Only Snapshot Access (query-engine, sim-scheduler)
The `bevy-bridge` SHALL obtain committed world data via `query-engine` to drive entity materialization and view model synchronization. The bridge:

- MUST NOT hold mutable references to Arrow tables.
- MUST NOT bypass `journal-system` to write diffs.
- MUST NOT reference raw Arrow `RecordBatch` or table internals directly.
- MUST NOT query the schema-registry `RelationGraph` directly.
- SHALL refresh read-only committed data after receiving `REFRESH_SIGNAL`.
- SHALL treat Bevy ECS components, view models, hover state, animation state, and frame-local caches as Derived or Ephemeral state, not Authority state.

**Component lifetime constraint:**
All Bevy Component types defined in `scharnhorst_bevy` SHALL satisfy `Component: 'static`. Fields containing string data SHALL use owned `String` rather than borrowed `&'static str`. The `ViewOf` component SHALL store:
- `row_id: RowId` (u64 wrapper -- `Copy`)
- `table_name: String` (owned -- NOT `&'static str`)
- `generation: u64`

Using `&'static str` for component fields is FORBIDDEN because it either requires `Box::leak` (memory leak) or a global string interner (complexity). `String` is the correct choice for Bevy Component fields that hold textual data.

**REFRESH_SIGNAL protocol:**

```
T+0 end of PostTick:
1. sim-scheduler broadcasts REFRESH_SIGNAL.
2. bevy-bridge receives signal.

T+1 before next frame sync:
3. bridge discards old committed data handles.
4. bridge obtains fresh read-only data via query-engine.
5. entity synchronization proceeds with generation N+1 data.
```

**Refresh handler error handling:**
The refresh handler SHALL handle `Mutex`/`RwLock` poisoning as a **recoverable error**, not a panic. Panicking via `.expect()` or `.unwrap()` on lock acquisition is FORBIDDEN. Instead, the handler SHALL use `.map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))` to propagate the error through the scheduler's refresh signal protocol.

#### Scenario: Bridge reads after commit
- **WHEN** `sim-scheduler` commits a tick and signals consumers
- **THEN** the bridge discards old committed data handles
- **AND** synchronizes entities from fresh query-engine data
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

### Requirement: Player Commands Only (journal-system)
The Bevy bridge SHALL ONLY buffer **player-originated** commands. AI and internal simulation commands bypass the bridge entirely and write directly to the `journal-system`. This ensures the bridge remains a UI-layer concern and does not become a bottleneck for simulation systems.

#### Scenario: AI declares war
- **WHEN** an AI system decides to declare war
- **THEN** it calls `journal_system.submit(Command::DeclareWar { ... })` directly, without going through the bridge buffer.

### Requirement: Bevy State Tier Ownership
The Bevy bridge SHALL document or register state-tier ownership for its runtime state:

- Player command buffer: Ephemeral until consumed into journal submission.
- Entity materialization registry: Derived or Ephemeral view mapping.
- View models and sync fields: Derived views of Authority state.
- Hover, selection, animation, and camera state: Ephemeral.

#### Scenario: Materialization registry rebuild
- **WHEN** a committed snapshot changes visible province ownership
- **THEN** Bevy view synchronization updates Derived view state from query-engine data
- **AND** the materialization registry is not treated as Authority state

### Requirement: No Simulation-Origin Commands Through Bridge
The Bevy bridge SHALL only buffer player-originated commands. AI and internal simulation systems submit directly to journal-system through scheduler-provided capabilities.

#### Scenario: AI declares war
- **WHEN** an AI system decides to declare war
- **THEN** it submits through journal-system or scheduler-provided journal capability
- **AND** it does not use the Bevy input buffer

### Requirement: Lock Poisoning Propagation
All Mutex lock acquisitions in `scharnhorst_bevy` production code SHALL propagate lock poisoning as `BevyBridgeError::LockPoisoned` rather than silently swallowing the poison via `if let Ok(...)` or `.ok()`. This includes `with_player_id` on `InputCommandBuffer` and all `SyncState` lock methods. Lock poisoning indicates an unrecoverable invariants violation in another subsystem and SHALL be surfaced to the caller.

#### Scenario: Poisoned lock surfaced
- **WHEN** any `scharnhorst_bevy` Mutex is poisoned by a panic in another subsystem
- **THEN** subsequent lock acquisitions return `BevyBridgeError::LockPoisoned`
- **AND** the error propagates to the application-level recovery or panic handler

---

## Invariants

### I-BB-GEN-SPLIT
`ViewModel::generation` and `ViewModelState::snapshot`/`latest_tick` are updated under separate locks. The microsecond window during `refresh()` where generation lags behind snapshot is harmless because no code couples them.

### I-BB-SIGNAL-ATOMIC
`signal_received` uses `Release` on store (in `on_refresh_signal`) and `Acquire` on load (in `should_refresh`). This forms a happens-before chain: the refresh signal setter's work happens-before the checker observes the flag.

### I-BB-GENERATION-ATOMIC
`last_snapshot_generation` uses `Relaxed` ordering. Since all mutations flow through a single Bevy system (the refresh handler), there is no need for inter-thread ordering — atomicity alone ensures correctness.

### I-BB-STATE-TIER
All Bevy ECS components, ViewModels, hover/animation/selection state, and frame-local caches are Derived or Ephemeral. The Bevy bridge never holds Authority state directly. Any state that must persist or be replay-authoritative lives in `ArrowStore` as Authority tables; the bridge materializes read-only views of that state.

### I-BB-INPUT-BOUNDARY
`InputCommandBuffer` contains only player-originated commands. AI and internal simulation systems SHALL submit commands directly to `journal-system` through scheduler-provided capabilities, bypassing the bridge buffer. No non-player command enters through the Bevy input path.

---

## Design Notes

### ViewModel Generation-Split Micro-Window (INFORMATIONAL)

**Location**: ViewModel — Snapshot Management

**Design**: The `generation` field is split from `ViewModelState` into a separate `AtomicU64`. During `refresh()`, `snapshot` and `latest_tick` are written under the Mutex first, then `generation` is stored to the atomic afterward. A concurrent `generation()` call in the microsecond window between the Mutex release and the atomic store observes the OLD generation with the NEW snapshot.

The design's analysis shows this is harmless because no production code couples the ViewModel's generation with its snapshot — all callers use them independently. Only test code reads the generation.

**No correction needed**. The existing analysis correctly identifies the window and verifies it is unobservable in production code. Documented here for completeness.

### SnapshotRefreshHandler `callback()` — Multiple Lock Bracket Eliminated (INFORMATIONAL)

**Location**: Refresh Handler — Signal Protocol

**Design**: After Phase 2 (broadcast callback snapshotting), the `callback()` closure is invoked outside the `RefreshSignalBus` consumers Mutex. With the atomic replacements in Phase 1, the two lock brackets in `callback()` become two atomic operations:

```rust
// Lock A → increment gen → Unlock A
// vm.refresh(...)
// Lock B → clear signal → Unlock B
```

With Phase 2, the callback is invoked while the consumers HashMap is unlocked — but the callback internally used atomics already. No additional risk. The two phases compose cleanly.

**No correction needed**. Documented as evidence of clean composition between Phase 1 and Phase 2.

---

## Implementation Notes

### ViewModel Atomic Replacement

The `ViewModel` struct splits `generation` from the Mutex-protected state:

```rust
struct ViewModelState {
    snapshot: Option<Arc<WorldSnapshot>>,
    latest_tick: Option<Tick>,
}

pub struct ViewModel {
    state: Arc<Mutex<ViewModelState>>,
    generation: Arc<AtomicU64>,    // split from Mutex
}
```

The `generation` store occurs before the `MutexGuard` is dropped — the atomic write does not block nor contend with the Mutex in any way. In normal operation, `generation` is observable concurrently with or slightly ahead of `snapshot`/`latest_tick` becoming visible.

### RefreshHandler Atomic Replacement

The `SnapshotRefreshHandler` eliminates `Mutex<RefreshHandlerState>` entirely:

```rust
pub struct SnapshotRefreshHandler {
    view_model: Arc<ViewModel>,
    query_engine: Arc<QueryEngine>,
    arrow_store: Arc<ArrowStore>,
    signal_received: Arc<AtomicBool>,         // was state.signal_received
    last_snapshot_generation: Arc<AtomicU64>, // was state.last_snapshot_generation
}
```

**Method changes:**
- `on_refresh_signal()`: `signal_received.store(true, Ordering::Release)`
- `should_refresh()`: `signal_received.load(Ordering::Acquire)`
- `refresh_snapshot()`: `fetch_add(1, Relaxed)` for generation, atomic store for signal clear
- `current_generation()`: `load(Relaxed)`
- `callback()`: two lock brackets become two atomic operations

### SyncState Lock Ordering Preserved

`SyncState` maintains its two-Mutex design (`view_models` before `dirty_entities`). The consistent ordering prevents deadlock:

```rust
let mut models = self.view_models.lock()...?;
let mut dirty = self.dirty_entities.lock()...?;
models.remove(&entity);
dirty.remove(&entity);
```

### InputCommandBuffer Single-Mutex Design

The input buffer uses a single `Mutex<InputBufferInner>` to prevent ABBA deadlocks.

