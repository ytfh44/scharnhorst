## Requirements

### Requirement: Phase-Based Execution
The system SHALL execute simulation systems in a strictly defined sequence of five phases: `PreTick` -> `Economy` -> `Diplomacy` -> `Military` -> `PostTick`.

#### Scenario: Order of execution
- **WHEN** starting a new tick
- **THEN** all systems in the `PreTick` phase complete before any system in the `Economy` phase begins.

#### Phase Propagation
Each system's `execute()` call SHALL include the current `Phase` value so the system knows which phase it is executing in. The `SimSystem::execute()` signature SHALL include a `phase: Phase` parameter in addition to `rng`, `query`, and `tick`. This enables systems that maintain per-phase state or validate correct phase assignment.

### Requirement: System Dependency Declaration
Each simulation system MUST declare its read and write dependencies on world tables.

#### Scenario: Parallel scheduling
- **WHEN** two systems are in the same phase and their write sets do not overlap
- **THEN** the scheduler MAY execute them in parallel. Sequential execution within a phase is ALWAYS correct. Parallel execution is an optimization, not a correctness requirement.

### Requirement: Deterministic RNG Stream
The system SHALL provide each system with a deterministic RNG stream based on the system ID and current tick.

#### Scenario: Replay consistency
- **WHEN** replaying a tick on a different machine
- **THEN** the RNG stream for "RandomEventSystem" produces the exact same sequence of values as the original run.

### Requirement: Tick-Boundary Command Consumption (bevy-bridge, journal-system)
At the start of each tick, the scheduler SHALL:
1. Pull all pending commands from the `bevy-bridge` `InputCommandBuffer`
2. Submit them to the `journal-system` as batch of `Command` objects
3. Only then begin executing simulation phases

No commands from the bridge are consumed mid-tick. Commands arriving after the tick boundary is processed are held until the next tick.

#### Scenario: Network input arrival
- **WHEN** a multiplayer client sends a "MoveArmy" command 50ms into a 200ms tick
- **THEN** the bridge buffers it; the scheduler picks it up at the start of the next tick.

### Requirement: Atomic Commit Trigger (journal-system, arrow-store)
At the end of the final phase (`PostTick`), the scheduler SHALL trigger `journal.commit()`, which:
1. Merges all diffs for a tick into a single atomic batch
2. Applies them to the `ArrowStore`, producing generation N+1
3. Publishes the new `WorldSnapshot`
4. Appends the diffs to the `SaveJournal` file

#### Snapshot Refresh Signal Protocol

After `journal.commit()` completes, the scheduler SHALL execute the following **tick boundary signal sequence** in strict order. Snapshot data is delivered via a **push model**: during `journal.commit()`, the `Journal` calls `query_engine.ingest_snapshot()` for every table, pushing the new generation's data into the query-engine's internal cache. Consumers do not pull snapshots -- they access the latest data through query-engine's typed read APIs. The bevy-bridge obtains a `WorldSnapshot` reference through `query_engine.snapshot()` for entity materialization purposes (this is a cached reference to the snapshot produced during commit, not a pull-based tick lookup).

```
T+0 (end of PostTick):
1. journal.commit() executes internally:
   a. Apply all pending diffs to ArrowStore
   b. Generate new WorldSnapshot (generation N+1)
   c. For each table: extract batches + position_map, call query_engine.ingest_snapshot(tick, name, batches, pos_map)
   d. Store WorldSnapshot in query-engine via store_world_snapshot()
2. journal.commit() returns CommitResult to scheduler
3. Scheduler broadcasts REFRESH_SIGNAL to all registered consumers:
   - sim-scheduler systems (internal)
   - rule-ir evaluator (via query-engine)
   - bevy-bridge (via query-engine)
4. Consumers MUST acknowledge signal before proceeding

T+1 (PreTick of next tick):
5. Consumers discard old cached references (generation N)
6. Rule-IR evaluator clears prefetch cache
7. Consumers access data from query-engine (which now serves generation N+1):
   - Rule-IR evaluator: uses lookup_row(), column_view(), batch_reader() -- typed read APIs
   - Bevy-bridge: obtains WorldSnapshot via query_engine.snapshot() for entity materialization
   - Sim-scheduler systems: use query-engine typed read APIs
8. Simulation begins with fresh snapshot
```

**Signal semantics**:
- **Synchronous**: All consumers must acknowledge before any proceeds to next tick
- **Atomic**: No consumer may begin T+1 operations with T-generation data
- **Ordered**: Steps 1->8 must occur in sequence; cache clear (step 6) MUST follow the availability of new data (step 1c ensured by journal.commit() completing before broadcast)
- **Error-preserving**: If a consumer's acknowledgement callback fails, the scheduler SHALL preserve the consumer's specific error (not replace it with a generic "not acknowledged" error). This enables diagnostics by propagating the root cause.

**Consumer contract**:
- On receiving `REFRESH_SIGNAL`, consumers MUST:
  1. Discard all cached references to generation N data
  2. Clear any prefetch caches (rule-ir) or derived data
  3. Access fresh data from query-engine (which already contains generation N+1 data pushed via `ingest_snapshot` during commit):
     - Typed read APIs (`lookup_row`, `column_view`, `batch_reader`, `read`) for most consumers
     - `query_engine.snapshot()` for bevy-bridge entity materialization
  4. Acknowledge signal to scheduler

**Refresh acknowledgement error type**:
```rust
enum SchedulerError {
    // ... existing variants ...
    RefreshSignalFailed {
        consumer: String,
        source: Box<SchedulerError>,
    },
}
```
When a consumer callback returns `Err(e)`, the bus SHALL wrap the specific error and propagate it. The `RefreshSignalNotAcknowledged` variant SHALL be removed.

**Consumer failure handling**:
- Consumers MUST handle lock poisoning and other recoverable errors by returning `Err` with a descriptive error
- Consumers MUST NOT panic via `.expect()` or `.unwrap()` on lock acquisition
- The scheduler SHALL NOT panic on any consumer failure -- errors propagate through `SchedulerResult`

**Registered Consumers**:
The following components MUST register for the REFRESH_SIGNAL:
- All simulation systems (via `sim-scheduler` internal registration)
- `rule-ir` evaluator (to clear prefetch cache per DC-7 in `rule-ir` spec)
- `bevy-bridge` (to refresh snapshot reference per DC-3 in `bevy-bridge` spec)

#### Determinism invariant:
For any tick T, `snapshot_T = apply_diffs(snapshot_{T-1}, journal_diffs_T)` is identical across all platforms and runs, given the same initial state and command sequence.

### Requirement: System Registration with Dependency Graph
Before `initialize()` is called, each simulation system MUST register with the scheduler, declaring:

- State tier classification: which state this system owns as Authority, Derived, or Ephemeral.
- Read tables: tables it will read via `query-engine`. Non-Authority tables SHALL NOT be listed — systems SHOULD access Derived/Ephemeral state through their own internal stores.
- Write tables: tables it will mutate (must be Authority, must go through journal diff submission).
- Refresh signal handler: callback function for REFRESH_SIGNAL with generation payload.

A system's declaration establishes its simulation capability boundary. A system owning authority tables SHALL declare itself as Authority and must write through journal diff submission. A system computing derived caches SHALL declare itself as Derived and must rebuild from query-engine reads. A system managing UI state SHALL declare itself as Ephemeral and must not access journal or ArrowStore.

All simulation systems MUST access Authority world state exclusively through the query-engine. Systems MUST NOT hold mutable references to Arrow tables, raw Arrow RecordBatch values, or partition internals. The query-engine is the unified read interface for all simulation consumers.

#### Scenario: System declares state tier classification
- **WHEN** a system is registered during scheduler initialization
- **THEN** its state tier, read tables (none if Ephemeral), write tables (none if non-Authority), and refresh handler are recorded
- **AND** the registration serves as compile-time-enforced documentation of the system's capability boundary

### Requirement: Scheduler Initialization Validation

The `Scheduler::initialize()` method SHALL validate the registration state before the first tick:

1. **Completeness check**: Verify every registered system has declared non-empty `read_tables()` or `write_tables()`
2. **Cycle detection (DEFERRED)**: With the current hardcoded linear phase order (PreTick -> Economy -> Diplomacy -> Military -> PostTick), cycles are structurally impossible. A proper cycle detection algorithm should be implemented if dynamic phase registration is added in the future. The `SchedulerError::DependencyCycle` variant exists but is never constructed in the current implementation.
3. **Write-conflict detection**: Detect pairs of systems registered in the same phase that write to the same table and produce an error
4. **Safe on re-entry**: Calling `initialize()` on an already-initialized scheduler SHALL be a no-op (not an error)

Validation failures SHALL return `SchedulerError` with descriptive messages. The scheduler SHALL NOT panic on invalid registration state.

The `Scheduler::tick()` method SHALL NOT panic. All failure modes (lock poisoning, commit failure, consumer refresh failure) SHALL propagate as `SchedulerError` variants through `SchedulerResult`. Panicking via `.unwrap()` or `.expect()` on Mutex/RwLock acquisition is FORBIDDEN anywhere in the scheduler crate.

### Requirement: Simulation Lifecycle Boundary
The simulation SHALL have exactly two lifecycle modes: Initialization and Simulation. All state mutations SHALL occur in the Initialization phase. Authority state mutations during Simulation SHALL only occur through journal-committed diffs.

#### Scenario: Initialization writes tables
- **WHEN** `content-loader` compiles base and mod definitions
- **THEN** it writes tables to `ArrowStore` through initialization capabilities
- **AND** no journal diffs are involved

#### Scenario: Simulation writes diffs
- **WHEN** a simulation system computes an economy update
- **THEN** it submits a diff through journal-system
- **AND** the diff is committed at the tick boundary

### Requirement: Refresh Signal Covers Non-Authority State
`REFRESH_SIGNAL` broadcast SHALL notify consumers regardless of their state tier. Both Derived and Ephemeral consumers MAY register handlers. The generation payload enables query-engine consumers to validate tick-synchronous data access.

#### Scenario: Derived consumer receives refresh
- **WHEN** `journal.commit()` completes and `REFRESH_SIGNAL` broadcasts
- **THEN** Derived state consumers receive the signal with the new generation number
- **AND** they may rebuild caches from query-engine data for the new generation

### Requirement: Tick-Commit Transactional Boundary
The scheduler's `tick()` method SHALL enforce that each tick follows exactly one PreTick->Economy->Diplomacy->Military->PostTick sequence and exactly one commit. Between the start of `tick()` and the completion of `journal.commit()`, NO external system may inject commands, diffs, or simulation state into the running tick.

#### Scenario: Atomic tick boundary
- **WHEN** `scheduler.tick()` is called
- **THEN** all consumer command buffers are drained at PreTick
- **AND** phases execute sequentially
- **AND** PostTick triggers a single commit
- **AND** the commit completes before the next `tick()` call

### Requirement: Deterministic RNG Across Platforms
Each system's RNG stream SHALL produce identical values across different platforms (Windows, Linux, macOS) and CPU architectures (x86, ARM) for the same system_id and tick, provided the simulator and RNG algorithms are consistent. This enables multiplayer desync-proofing and deterministic replay on any platform.

`deterministic_rng` SHALL use a fixed-endian seed encoding for the RNG state. Little-endian encoding SHALL be used on all platforms to ensure cross-platform identical output. The encoded seed bytes SHALL be the same regardless of the host platform's native byte order.

#### Scenario: Cross-platform RNG consistency
- **WHEN** a simulation runs tick 42 on Windows with `system_id = 7`
- **THEN** the RNG output matches the same `(system_id=7, tick=42)` run on Linux and macOS
- **AND** replay verification across platforms is guaranteed

### Requirement: Registration Lifecycle Gate
Before the first tick, each simulation system SHALL register with the scheduler. `register_system()` SHALL be callable from any thread via thread-safe data structures. `register_system()` called on an already-initialized scheduler (where `is_initialized() == true`) SHALL return `SchedulerError::AlreadyInitialized` to catch programming errors during debugging while being a recoverable error in production.

The `run()` method SHALL require `is_initialized() == true` before executing ticks. Calling `run()` on an uninitialized scheduler SHALL return `SchedulerError::NotInitialized` rather than panicking. This TOCTOU-safe gate ensures systems are registered before simulation begins.

#### Scenario: Late registration rejected
- **WHEN** a system attempts to register after `initialize()` has completed
- **THEN** `register_system()` returns `SchedulerError::AlreadyInitialized`
- **AND** the scheduler state remains unchanged

#### Scenario: Run without initialization rejected
- **WHEN** `scheduler.run()` is called before `initialize()`
- **THEN** `SchedulerError::NotInitialized` is returned
- **AND** no ticks are executed

### Requirement: Unregistration Cleans Refresh Bus
When a system is removed from registration (e.g., mod unload or configuration change), the scheduler SHALL remove its refresh callback from the signal bus. Stale refresh callbacks referencing deallocated state or invalidated thread-local data SHALL NOT persist after unregistration. This prevents segmentation faults and use-after-free in the refresh broadcast loop.

#### Scenario: Unregister removes stale callback
- **WHEN** a system calls `unregister_system()`
- **THEN** its refresh handler is removed from the signal bus
- **AND** the next `REFRESH_SIGNAL` broadcast does not invoke the removed handler
- **AND** no stale reference persists in the consumer list

---

## Invariants

### I-SCHED-TICK-ATOMIC
`current_tick` and `generation` are atomically accessed with `Ordering::Relaxed`. Since all mutations occur on the single scheduler thread, there is no memory ordering requirement beyond atomicity. This `generation` counter is independent of the ArrowStore's `generation` counter (see arrow-store spec I-AS-GENERATION): the Scheduler increments in `atomic_commit()` after journal commit, while the ArrowStore increments at the start of `generate_snapshot()` called *within* the journal commit. The two have no happens-before relationship.

### I-SCHED-INIT-ACQUIRE
`initialized` uses `Ordering::Acquire` via the `load`-then-`store` two-phase pattern — not via `compare_exchange`. The `Acquire` load on the fast-path check pairs with the `Release` store at the end of successful validation, ensuring all validation side-effects are visible to any thread that subsequently observes `is_initialized() == true`.

### I-SCHED-BROADCAST-SNAPSHOT
`RefreshSignalBus::broadcast()` operates on a cloned snapshot of the consumer list, making it safe for `register`/`unregister` to run concurrently.

### I-SCHED-LIFECYCLE-GATE
`register_system()` called on an already-initialized scheduler (`is_initialized() == true`) SHALL return `SchedulerError::AlreadyInitialized`. `run()` called before `initialize()` SHALL return `SchedulerError::NotInitialized`. Both gates are TOCTOU-safe: the `initialized` flag uses `Acquire`/`Release` ordering, and `run()` checks the flag on each call. This prevents simulation from starting with unregistered systems and catches late-registration programming errors.

### I-SCHED-RNG-PLATFORM
Each system's RNG stream produces identical values across Windows/Linux/macOS and x86/ARM for the same `(system_id, tick)` pair, provided the simulator and RNG algorithms are consistent. `deterministic_rng` uses little-endian seed encoding on all platforms, regardless of host native byte order. This enables multiplayer desync-proofing and cross-platform replay verification.

### I-SCHED-UNREGISTER
`unregister_system()` removes the system's refresh callback from the signal bus. After unregistration, `REFRESH_SIGNAL` broadcasts SHALL NOT invoke the removed handler. No stale callback reference persists in the consumer list. This prevents segmentation faults or use-after-free when systems are removed.

---

## Design Notes

### `initialize()` CAS — Permanent "Initialized" on Validation Failure (SIGNIFICANT)

**Location**: Initialization

**Problem**: The proposed `compare_exchange` pattern sets `initialized = true` BEFORE validation runs:

```rust
if self.initialized.compare_exchange(false, true, Acquire, Relaxed).is_err() {
    return Ok(());  // already initialized
}
// ... validation runs with initialized=TRUE already stored
```

If validation fails, `initialized` is already `true`. Subsequent calls return `Ok(())` immediately — the scheduler is permanently "initialized" with failed validation. The old Mutex design held the lock through validation AND the flag write, so a validation failure never set the flag.

The design argues this is safe because "initialize() is called once at application startup." However:
- The Bevy bridge may call `initialize()` from Bevy system setup, where `RefreshSignalBus::register()` runs concurrently (after Phase 2).
- Any future parallel startup code path triggers this bug.

**Impact**: Validation failures are masked permanently. The scheduler reports itself as initialized with potentially incorrect system registrations.

**Correction Applied**: Replaced the compare_exchange with a two-phase pattern:

```rust
pub fn initialize(&self) -> SchedulerResult<()> {
    // Fast path: already initialized
    if self.initialized.load(Ordering::Acquire) {
        return Ok(());
    }

    // Validation (must succeed before setting flag)
    let systems = self.lock_systems()?;
    let registrations = self.lock_registrations()?;
    // ... existing validation logic ...

    // Only now mark as initialized
    self.initialized.store(true, Ordering::Release);
    Ok(())
}
```

This preserves the idempotency contract while ensuring validation failures are never masked.

---

## Implementation Notes

### Atomic Replacements

The `Scheduler` replaces three `Arc<Mutex<T>>` fields with atomics:

```rust
current_tick: Arc<AtomicU64>,
generation: Arc<AtomicU64>,
initialized: Arc<AtomicBool>,
```

### Tick Lifecycle

```rust
pub fn current_tick(&self) -> SchedulerResult<Tick> {
    Ok(Tick(self.current_tick.load(Ordering::Relaxed)))
}

pub fn advance_tick(&self) -> SchedulerResult<Tick> {
    let next = self.current_tick.fetch_add(1, Ordering::Relaxed);
    let new_tick = Tick(next).next();
    Ok(new_tick)
}
```

The use of `.next()` ensures the return value follows `Tick`'s saturation semantics at the type level.

### Atomic Commit

```rust
self.generation.fetch_add(1, Ordering::Relaxed);
```

`fetch_add` uses wrapping arithmetic (u64 overflow is impossible in practice).

### Initialization Two-Phase Pattern

```rust
pub fn initialize(&self) -> SchedulerResult<()> {
    // Phase 1: fast-path check
    if self.initialized.load(Ordering::Acquire) {
        return Ok(());
    }
    
    // Phase 2: validation under Mutex serialization
    let systems = self.lock_systems()?;
    let registrations = self.lock_registrations()?;
    // ... validation ...
    
    self.initialized.store(true, Ordering::Release);
    Ok(())
}
```

**Why not compare_exchange**: A `compare_exchange(false, true, Acquire, Relaxed)` would permanently mask validation failures: if the CAS succeeded but validation subsequently failed, the `initialized` flag would remain `true` while no actual initialization completed. The two-phase pattern uses `Acquire`/`Release` ordering matching the original Mutex semantics but avoids this masking issue.

### Refresh Signal Bus — Broadcast Callback Snapshotting

The `RefreshSignalBus::broadcast()` clones the consumer list under the lock, then releases the lock before iteration:

```rust
pub fn broadcast(&self, tick: u64, generation: u64) -> SchedulerResult<()> {
    let callbacks: Vec<_> = {
        let consumers = self.consumers.lock()?;
        consumers.values().cloned().collect()
    }; // lock released here
    
    for cb in &callbacks {
        cb(tick, generation)?;
    }
    Ok(())
}
```

**Observable behavioral change**: Register/unregister calls during a broadcast's callback execution succeed immediately without blocking on the Mutex. Newly registered consumers do NOT receive the current broadcast; newly unregistered consumers DO receive it.

### Debug Implementation

The `Debug` impl uses atomic loads for `current_tick` and `generation` (no lock contention). Under `AtomicU64`, there is no poison state — `load()` always succeeds.

### Lock Helpers Removal

The lock helper methods `lock_tick`, `lock_generation`, `lock_initialized` are removed (they were private). The remaining lock helpers (`lock_systems`, `lock_registrations`, `lock_commands`, `lock_journal`) protect complex multi-field state.

