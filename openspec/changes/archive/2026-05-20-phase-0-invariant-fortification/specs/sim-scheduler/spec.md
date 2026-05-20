## MODIFIED Requirements

### Requirement: System Registration with Dependency Graph
Before the first tick, each simulation system MUST register with the scheduler, declaring:

- Read tables or query paths it will access through `query-engine`.
- Write diff targets it may submit through the journal submission capability.
- Refresh signal handler if the system maintains caches, Derived state, or tick-scoped Ephemeral state.
- State-tier ownership for any Derived or Ephemeral data it maintains.

All simulation systems MUST access Authority state exclusively through `query-engine`. Systems MUST NOT receive raw `ArrowStore` mutation capabilities, initialization capabilities, or journal commit capabilities during tick execution.

In debug builds only, a system MAY issue SQL write statements through the query-engine debug interface. These statements MUST be intercepted by `DebugWriteJournal` and translated into journal diffs.

The scheduler uses registration data to:

- Order phases topologically where applicable.
- Parallelize systems within a phase when write sets are disjoint.
- Detect write conflicts.
- Build the `REFRESH_SIGNAL` recipient list.
- Validate Derived and Ephemeral invalidation behavior.

#### Scenario: Scheduler injects narrow capabilities
- **WHEN** the scheduler executes an Economy system
- **THEN** the system receives the current phase, deterministic RNG, query capability, and journal submission capability
- **AND** it does not receive an `ArrowStore` mutation capability

#### Scenario: Write conflict detection remains journal-oriented
- **WHEN** two systems in the same phase declare writes to the same Authority table
- **THEN** scheduler initialization returns a write-conflict error
- **AND** neither system runs until registration is corrected

### Requirement: Atomic Commit Trigger (journal-system, arrow-store)
At the end of the final phase (`PostTick`), the scheduler SHALL trigger `journal.commit()`. The scheduler itself MUST NOT apply diffs to `ArrowStore`.

Commit SHALL:
1. Merge all diffs for the tick into the journal commit operation.
2. Let `journal-system` apply them to Authority state through the journal-owned commit capability.
3. Publish the new committed snapshot through `query-engine`.
4. Append the diffs to `SaveJournal` where configured.
5. Broadcast `REFRESH_SIGNAL` only after commit succeeds, passing the new generation number as the signal payload.

The `atomic_commit()` method (used for replay and integration testing) SHALL follow the same sequence: commit diffs, then broadcast `REFRESH_SIGNAL` with the current generation number. The generation counter SHALL be incremented during `atomic_commit()` after the commit succeeds but before the broadcast.

`generation` atomic reads SHALL use `Acquire` ordering and writes SHALL use `Release` (or
`AcqRel` on `fetch_add`) to form a release-acquire pair. A reader that observes a new
generation value through `current_generation()` with `Acquire` is guaranteed to see all
committed data written before the corresponding `fetch_add` with `AcqRel`.

#### Scenario: Scheduler does not mutate store
- **WHEN** the scheduler reaches the tick boundary
- **THEN** it calls `journal.commit()`
- **AND** it does not call `ArrowStore::apply_diffs` or any structural store mutation directly

#### Scenario: Commit failure blocks refresh
- **WHEN** `journal.commit()` returns an error
- **THEN** the scheduler returns the error
- **AND** it does not broadcast `REFRESH_SIGNAL` for an uncommitted generation
- **AND** the generation counter is not incremented

## ADDED Requirements

### Requirement: Simulation Lifecycle Boundary
The scheduler SHALL start ticks only after Initialization is complete, the schema registry is frozen, Authority tables are constructed, and simulation systems are initialized with narrow capabilities.

#### Scenario: Starting first tick
- **WHEN** the scheduler starts the first tick
- **THEN** `schema-registry.is_frozen()` is true
- **AND** the scheduler no longer exposes initialization capabilities to systems

### Requirement: Refresh Signal Covers Non-Authority State
The scheduler's `REFRESH_SIGNAL` protocol SHALL invalidate or rebuild registered Derived state and discard tick-scoped Ephemeral state according to each consumer's contract.

#### Scenario: Derived state handler runs after commit
- **WHEN** a tick commit succeeds and generation N+1 is published
- **THEN** the scheduler broadcasts `REFRESH_SIGNAL`
- **AND** registered Derived state handlers mark themselves dirty or rebuild against generation N+1

#### Scenario: Ephemeral tick cache discarded
- **WHEN** a system owns a tick-scoped Ephemeral cache
- **THEN** its refresh handler discards the cache before the next tick begins

### Requirement: Tick-Commit Transactional Boundary
The scheduler's `tick()` method SHALL treat command consumption, phase execution, and journal commit as a transactional boundary. If `journal.commit()` fails after phases have executed and produced diffs, the failure SHALL be propagated to the caller. The scheduler SHALL NOT advance the tick or broadcast `REFRESH_SIGNAL` after a failed commit.

Commands consumed at tick start SHALL NOT be re-consumed on retry: if `tick()` fails, the caller is responsible for deciding retry policy. The scheduler does not automatically roll back consumed commands.

The generation counter SHALL be incremented only after a successful `journal.commit()` and before `REFRESH_SIGNAL` broadcast. If `broadcast_refresh` itself fails, the generation has already been incremented and the caller is responsible for recovery.

#### Scenario: Failed commit does not advance tick
- **WHEN** a tick executes phases successfully but `journal.commit()` fails
- **THEN** `tick()` returns the commit error
- **AND** the scheduler's `current_tick` is not advanced
- **AND** the generation counter is not incremented
- **AND** `REFRESH_SIGNAL` is not broadcast

### Requirement: Deterministic RNG Across Platforms
The scheduler's per-system RNG streams SHALL produce identical sequences across all platforms and Rust compiler versions. The RNG algorithm SHALL use a fixed hash function (e.g., `splitmix64` with explicit state management) rather than `std::collections::hash_map::DefaultHasher`, which does not guarantee cross-platform stability.

#### Scenario: RNG replay across platforms
- **WHEN** the same simulation is replayed on Linux and Windows with the same initial seed
- **THEN** every system's RNG produces the exact same sequence of values
- **AND** the final state hash matches

### Requirement: Registration Lifecycle Gate
`register_system()` SHALL reject registration when `initialized` is `true`.
The initialized check SHALL be re-performed AFTER acquiring the `systems` and
`registrations` locks to eliminate a TOCTOU window between the initial check
and lock acquisition, which could allow a concurrent `initialize()` to slip
a registration past the gate.

#### Scenario: Late registration rejected
- **WHEN** a system attempts `register_system()` after `initialize()` has completed
- **THEN** the registration returns `SchedulerError::Initialized`
- **AND** the scheduler's system set remains unchanged

### Requirement: Unregistration Cleans Refresh Bus
`Scheduler::unregister_system()` SHALL remove the system from all internal registries including `systems`, `registrations`, and `refresh_bus`. After unregistration, the system's refresh callback SHALL NOT be invoked during subsequent `broadcast_refresh()` calls. Failing to clean the refresh bus leaves a dangling callback that may reference deallocated or moved state.

#### Scenario: Unregistered system callback not invoked
- **WHEN** a system with a refresh callback is unregistered
- **THEN** subsequent `broadcast_refresh()` calls do not invoke the removed callback
- **AND** the system name no longer appears in the scheduler's system list
