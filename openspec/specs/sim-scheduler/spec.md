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
Before the first tick, each simulation system MUST register with the scheduler, declaring:
- **Read tables**: tables it will read via `query-engine`
- **Write diffs**: the `Diff` types it may emit (e.g., `EconomySystem` emits `Diff::Update` on "economy" table)
- **Refresh signal handler**: callback function for REFRESH_SIGNAL (if the system maintains any cache or derived state)

All simulation systems MUST access world state **exclusively through the `query-engine`** -- whether for semantic-aware reads, filtered scans, or direct column access. The query-engine is the **unified read interface** for all consumers (simulation systems, `bevy-bridge`, `rule-ir` evaluator). Internally, the query-engine may dispatch to different Arrow access strategies (direct column scan, index lookup, SQL plan) depending on the query type, allowing the Arrow layer to perform parallel optimizations without the caller needing to know the strategy used.

In **debug builds only**, a system MAY issue SQL `UPDATE`/`INSERT` statements through the `query-engine` SQL interface. These are intercepted by the `DebugWriteJournal` (see `query-engine` spec) and translated into journal diffs, preserving the single-write-entry-point invariant. This path is **disabled in production and multiplayer builds**.

The scheduler uses registration data to:
- Order phases topologically
- Parallelize systems within a phase when write sets are disjoint (optimization -- sequential is always correct)
- Detect conflicts (two systems writing to the same table in the same phase = error)
- Build the REFRESH_SIGNAL recipient list

### Requirement: Scheduler Initialization Validation

The `Scheduler::initialize()` method SHALL validate the registration state before the first tick:

1. **Completeness check**: Verify every registered system has declared non-empty `read_tables()` or `write_tables()`
2. **Cycle detection (DEFERRED)**: With the current hardcoded linear phase order (PreTick -> Economy -> Diplomacy -> Military -> PostTick), cycles are structurally impossible. A proper cycle detection algorithm should be implemented if dynamic phase registration is added in the future. The `SchedulerError::DependencyCycle` variant exists but is never constructed in the current implementation.
3. **Write-conflict detection**: Detect pairs of systems registered in the same phase that write to the same table and produce an error
4. **Safe on re-entry**: Calling `initialize()` on an already-initialized scheduler SHALL be a no-op (not an error)

Validation failures SHALL return `SchedulerError` with descriptive messages. The scheduler SHALL NOT panic on invalid registration state.

The `Scheduler::tick()` method SHALL NOT panic. All failure modes (lock poisoning, commit failure, consumer refresh failure) SHALL propagate as `SchedulerError` variants through `SchedulerResult`. Panicking via `.unwrap()` or `.expect()` on Mutex/RwLock acquisition is FORBIDDEN anywhere in the scheduler crate.