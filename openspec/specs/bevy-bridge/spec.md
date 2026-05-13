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

The `bevy-bridge` SHALL obtain an immutable `WorldSnapshot` reference **via the `query-engine`** to drive ViewModel synchronization. The bridge:
- MUST NOT hold mutable references to Arrow tables.
- MUST NOT bypass the `journal-system` to write diffs.
- MUST NOT reference Arrow `RecordBatch` or `Table` objects directly; all reads MUST go through `query-engine`.
- MUST NOT query the `schema-registry`'s RelationGraph directly (accessed via `query-engine`).
- SHALL refresh its snapshot reference **after** receiving the refresh signal from `sim-scheduler` (triggered by `journal.commit()` at tick boundary), discarding the prior snapshot.

#### Component Lifetime Constraint:
All Bevy Component types defined in `scharnhorst_bevy` SHALL satisfy `Component: 'static`. Fields containing string data SHALL use `String` (owned heap-allocated) rather than `&'static str`. The `ViewOf` component SHALL store:
- `row_id: RowId` (u64 wrapper -- `Copy`)
- `table_name: String` (owned -- NOT `&'static str`)
- `generation: u64`

Using `&'static str` for component fields is FORBIDDEN because it either requires `Box::leak` (memory leak) or a global string interner (complexity). `String` is the correct choice for Bevy Component fields that hold textual data.

#### REFRESH_SIGNAL Protocol

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

#### Refresh handler error handling:
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

### Requirement: Player Commands Only (journal-system)
The Bevy bridge SHALL ONLY buffer **player-originated** commands. AI and internal simulation commands bypass the bridge entirely and write directly to the `journal-system`. This ensures the bridge remains a UI-layer concern and does not become a bottleneck for simulation systems.

#### Scenario: AI declares war
- **WHEN** an AI system decides to declare war
- **THEN** it calls `journal_system.submit(Command::DeclareWar { ... })` directly, without going through the bridge buffer.

---

## Invariants

### I-BB-GEN-SPLIT
`ViewModel::generation` and `ViewModelState::snapshot`/`latest_tick` are updated under separate locks. The microsecond window during `refresh()` where generation lags behind snapshot is harmless because no code couples them.

### I-BB-SIGNAL-ATOMIC
`signal_received` uses `Release` on store (in `on_refresh_signal`) and `Acquire` on load (in `should_refresh`). This forms a happens-before chain: the refresh signal setter's work happens-before the checker observes the flag.

### I-BB-GENERATION-ATOMIC
`last_snapshot_generation` uses `Relaxed` ordering. Since all mutations flow through a single Bevy system (the refresh handler), there is no need for inter-thread ordering — atomicity alone ensures correctness.

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

