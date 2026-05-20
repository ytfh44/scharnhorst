## Requirements

### Requirement: Command Logging
The system SHALL capture all external and internal intents as `Command` objects before they affect the world state.

#### Scenario: Player input
- **WHEN** a player transfers control of a province
- **THEN** a `TransferControl` command is appended to the command journal with a timestamp and player ID.

### Requirement: Diff Generation
The simulation systems MUST produce `Diff` objects rather than mutating tables directly.

#### Scenario: Treasury update
- **WHEN** a system calculates tax income for an actor
- **THEN** it emits a `Diff::Update` for the "treasury" column of that actor's row.

### Requirement: Atomic Commit
The system SHALL apply all pending diffs for a tick atomically through the journal-owned commit capability, producing a new world generation and a single state hash.

Commit SHALL:
1. Reject re-entry if a commit is already in progress.
2. Transition phase to `Committing`.
3. Take pending diffs and commands for the current tick.
4. Apply diffs to Authority state through the commit capability.
5. Generate or publish the committed Authority snapshot.
6. Publish snapshot data to `query-engine`.
7. Append the commit record to `SaveJournal` if configured.
8. Push the commit record to history.
9. Transition phase to `Committed`, then back to `Open`.
10. Advance the open tick.

Commit failures SHALL return `JournalError` without panic. On any failure after step 2, the journal SHALL:
- Reset the phase back to `Open`.
- Restore all pending diffs and commands that were taken in step 3, so they remain available for retry.
- NOT advance the tick.
- NOT append to history.

This ensures a failed commit leaves the journal in a consistent state ready for retry, rather than a permanently deadlocked `Committing` phase with lost diffs.

#### Scenario: State commit
- **WHEN** the simulation reaches the end of the tick
- **THEN** all pending diffs are merged, applied through the journal-owned commit capability, and a new `WorldSnapshot` is published
- **AND** the commit result includes the committed tick, diff count, and state hash

#### Scenario: Commit failure preserves policy
- **WHEN** diff application fails because a target table is missing
- **THEN** `journal.commit()` returns an error
- **AND** the phase resets to `Open` so the journal can accept retries
- **AND** pending diffs and commands remain available for the next commit attempt
- **AND** the scheduler or application decides recovery policy
- **AND** the journal does not panic

#### Commit Failure Recovery Invariants

On any commit() failure, the journal SHALL restore pending diffs and commands to their
pre-commit state and reset the phase to `Open`, ensuring the caller can safely re-attempt
the commit.

- SHALL on `apply_diffs_and_hash` failure: restore `pending_diffs` and `pending_commands`,
  reset `phase` to `Open`, return error without advancing tick
- SHALL on query-engine snapshot ingestion failure: restore `pending_diffs` and `pending_commands`,
  reset `phase` to `Open`, return error without advancing tick
- SHALL on save-journal append failure: destructure `CommitRecord` to recover
  `diffs` and `commands`, restore them, reset `phase` to `Open`, return error without advancing tick

Note: ArrowStore mutations from `apply_diffs_and_hash` are NOT rolled back on downstream
failures (query-engine or save-journal). Diffs are already applied to the store before those steps.
Restored pending diffs may be re-applied on re-commit. Updates are idempotent; inserts will
fail with DuplicateRowId if the row was already created in the partial commit.

#### Scenario: Commit failure after ArrowStore mutation
- **WHEN** diff application succeeds but subsequent query-engine ingestion fails during commit
- **THEN** the journal returns an error describing the ingestion failure
- **AND** the phase resets to `Open` with pending diffs and commands restored
- **AND** the ArrowStore has been mutated by the committed diffs — the application is responsible for reconciling the inconsistency, typically by discarding the tick data and replaying from the last known good snapshot

#### Scenario: Commit failure restores pending diffs
- **WHEN** `apply_diffs_and_hash()` returns an error during commit
- **THEN** `pending_diffs` and `pending_commands` are restored to their pre-commit state
- **AND** the journal phase resets to `Open`
- **AND** the tick does not advance
- **AND** the caller may re-attempt the commit with the same pending diffs and commands

### Requirement: Single Write Entry Point (arrow-store, bevy-bridge, rule-ir, sim-scheduler)
The journal system SHALL be the sole simulation write path for all Authority world state mutations. No component may write directly to Arrow tables. The following write sources are defined:

| Source | Write Path |
|--------|-----------|
| Player commands via `bevy-bridge` | `bevy-bridge.InputCommandBuffer` -> `sim-scheduler` tick start -> `journal.submit()` |
| AI and internal systems | System logic -> journal submission capability |
| Rule-IR effects | Evaluator effect phase -> journal submission capability |
| Debug/developer writes in debug builds only | `DebugWriteJournal` -> journal submission capability |

The journal SHALL own the only commit capability that can apply simulation diffs to `ArrowStore`. Systems and debug tools may submit commands, effects, or diffs, but they MUST NOT receive the commit capability and MUST NOT apply diffs directly.

**Invariant**: At any point, the published Authority snapshot reflects all committed diffs up to generation N, and no uncommitted diffs have been applied to Authority state.

**DebugWriteJournal implementation:**

The `DebugWriteJournal` is a debug-only component that intercepts SQL `UPDATE`, `INSERT`, and `DELETE` statements from developer tooling and translates them into standard `Diff` objects:

```
Debug Build Path:
  Developer SQL
    -> query-engine SQL parser
    -> DebugWriteJournal intercepts
    -> translate to Diff
    -> submit to journal-system
    -> commit at tick boundary
```

**Constraints**:
- Debug builds only: disabled in production and multiplayer builds.
- No direct writes: still routes through journal.
- Tick-aligned: changes are committed at the next tick boundary.
- Logged: all debug writes are logged for debugging reproducibility.

#### Scenario: Player command writes through journal
- **WHEN** a player command changes province control
- **THEN** the command is submitted to the journal at tick start
- **AND** any resulting Authority state diff is applied only during journal commit

#### Scenario: Debug SQL does not bypass journal
- **WHEN** a developer runs `UPDATE actor_state SET treasury = 1000 WHERE actor_id = 1` in a debug console
- **THEN** the SQL is translated into a diff and submitted to the journal
- **AND** `ArrowStore` is not mutated until the tick-boundary commit

### Requirement: Tick-Aligned Commit Cycle (sim-scheduler, bevy-bridge, query-engine)
Commands consumed from the bridge buffer at tick start are submitted to the journal **before** simulation systems execute. All diffs produced during the tick are batched and committed **atomically** at tick end.

```
Tick lifecycle:
T+0 (start):
  1. scheduler pulls bridge commands -> journal.submit(commands)
  2. scheduler runs [PreTick, Economy, Diplomacy, ...PostTick] systems
  3. systems emit Diffs -> journal accumulates

T+1 (end of PostTick):
  4. scheduler triggers journal.commit()
  5. diffs applied atomically to ArrowStore -> new snapshot (generation N+1) published
  6. scheduler broadcasts REFRESH_SIGNAL to all consumers (sim-scheduler systems, rule-ir evaluator, bevy-bridge)
  7. consumers acknowledge signal (discard old snapshot references, clear caches)
  8. SaveJournal appended
```

**See**: `sim-scheduler` for the complete REFRESH_SIGNAL protocol.

### Requirement: SaveJournal for Incremental Persistence (save-system)
Each tick's accumulated diffs SHALL be serialized to the `SaveJournal` (append-only IPC file) immediately after commit. The SaveJournal is a flat sequence of `(tick_number, Vec<Diff>)` entries.

#### Scenario: Incremental save
- **WHEN** the player quick-saves
- **THEN** the system writes the SaveJournal since the last checkpoint. No full snapshot is generated.
- **WHEN** the player does a full save
- **THEN** the system writes a full `WorldSnapshot` and truncates the SaveJournal.

### Requirement: Submission Capability Separation
The journal system SHALL distinguish diff submission from diff application. Submission capabilities may be passed to systems and evaluators; commit capabilities SHALL remain private to the scheduler/journal commit path.

- The `JournalSubmitToken` SHALL be constructible via `pub fn new()`, serving as a documentation marker that the caller is authorized to submit. While a `pub(crate)` constructor would provide stronger compile-time enforcement, Rust crate visibility rules make this impractical when framework crates (arrow-store, journal, scheduler) need the token and external crates should not. Defense-in-depth against unauthorized submission is enforced at the guard layer: `CommitStore::new()` requires `Arc<ArrowStore>` which external simulation consumers SHALL NOT receive; `inner()` on both `InitStore` and `CommitStore` is removed to prevent `Arc<ArrowStore>` leakage through the capability boundary; `ArrowStore::advance_to_simulation()` is `pub(crate)`; `InitStore::into_simulation()` is the sole public path to obtain a `CommitStore`. Systems receive `Journal::submit_command()` and `Journal::submit_diff()` which accept `JournalSubmitToken` as proof of authorization; systems never receive a `CommitToken`.

#### Scenario: System emits a diff
- **WHEN** an Economy system computes a treasury change
- **THEN** it submits a diff through the journal submission capability
- **AND** it cannot call the commit capability or mutate `ArrowStore`

#### Scenario: External crate must obtain submit token through scheduler
- **WHEN** a test in a different crate needs to submit to the journal
- **THEN** the submit token is constructible via `JournalSubmitToken::new()` (it is `pub`, not `pub(crate)`)
- **AND** the only safe path to obtain a submit token is through the scheduler's system execution context
- **AND** defense-in-depth is enforced at the guard layer: external crates SHALL NOT receive `Arc<ArrowStore>`, preventing construction of `CommitStore`

### Requirement: Clear Pending Guards Phase
`Journal::clear_pending()` SHALL only be callable when the journal phase is `Open`. Calling `clear_pending()` during `Committing` or `Committed` phase SHALL return `JournalError::InvalidPhase` with a descriptive message. This prevents accidental clearing of diffs that are being committed or have just been committed. The `JournalError::InvalidPhase` variant SHALL exist as a dedicated error variant distinct from `SubmitFailed`.

#### Scenario: Clear pending during commit rejected
- **WHEN** `clear_pending()` is called while a commit is in progress
- **THEN** the call returns `JournalError::InvalidPhase`
- **AND** the pending diffs and commands remain unchanged

### Requirement: Command Persistence in Commit Record
`CommitRecord` SHALL include both pending diffs and pending commands for the committed tick. When replaying journal history, commands provide the semantic intent behind diffs; omitting commands from the record loses this intent.

#### Scenario: Commit record contains commands
- **WHEN** a tick commit completes
- **THEN** the `CommitRecord` appended to history contains both the applied diffs and the original submitted commands
- **AND** replaying the commit record reconstructs both the diff sequence and the command context

### Requirement: Tick Saturates at MAX
`Tick::next()` SHALL saturate at `Tick::MAX` rather than wrapping to 0, preserving monotonicity under overflow. `Tick::MAX` is defined as `Tick(u64::MAX - 1)`, with `Tick(u64::MAX)` reserved as a sentinel. `Tick::MAX.next()` SHALL return `Tick::MAX`, not wrap to 0. This ensures that `Tick::MAX` serves as a reliable upper bound for all boundary checks.

#### Scenario: Tick saturates at MAX
- **WHEN** calling `next()` on `Tick::MAX`
- **THEN** the result is `Tick::MAX` (saturating, not wrapping)
- **AND** tick monotonicity is preserved
- **AND** no tick value equals `Tick(u64::MAX)` (sentinel)

### Requirement: Journal Replay Uses Commit Semantics
Save/load replay after a checkpoint SHALL apply post-checkpoint diffs using the same journal-owned Authority mutation semantics as live simulation.

#### Scenario: Loading checkpoint and replay journal
- **WHEN** `save-system` loads snapshot N and replays diffs for ticks N+1 through M
- **THEN** replay uses the journal-owned commit path or an equivalent replay capability with the same invariants
- **AND** the final Authority state hash matches the saved expected hash

### Requirement: Commit History Bounding
The in-memory commit history (`VecDeque<CommitRecord>`) SHALL be bounded to prevent unbounded memory growth during long-running simulations. The maximum history length SHALL be a compile-time constant with a default of `1024` entries. When the history exceeds this bound, the oldest entries SHALL be evicted via `pop_front`.

#### Scenario: History eviction on overflow
- **WHEN** the commit history reaches `MAX_COMMIT_HISTORY + 1` entries
- **THEN** the oldest entry is evicted via `pop_front`
- **AND** the history length never exceeds `MAX_COMMIT_HISTORY`
- **AND** the most recent `MAX_COMMIT_HISTORY` entries remain available for replay and inspection

---

## Invariants

### I-JRN-SINGLE-WRITE
The journal is the sole write path for all Authority state mutations. All commands, diffs, and debug writes route through the journal. At any point, the published Authority snapshot reflects all committed diffs up to generation N, and no uncommitted diffs have been applied to Authority state.

### I-JRN-COMMIT-RECOVERY
On any `commit()` failure after phase transition to `Committing`, the journal SHALL:
- Reset phase to `Open`.
- Restore all pending diffs and commands that were taken, so they remain available for retry.
- NOT advance the tick.
- NOT append to commit history.

A failed commit leaves the journal in a consistent state ready for retry. ArrowStore mutations from `apply_diffs_and_hash` are NOT rolled back on downstream failures (query-engine or save-journal); restored pending diffs may re-apply on re-commit. Updates are idempotent; inserts fail with DuplicateRowId if the row was created in the partial commit.

### I-JRN-SUBMIT-SEPARATION
`JournalSubmitToken` is `pub` (constructible by callers), serving as a documentation marker of authorization to submit. Defense-in-depth is enforced at the guard layer: `CommitStore::new()` requires `Arc<ArrowStore>` which external simulation consumers SHALL NOT receive; `InitStore` and `CommitStore` do not expose `inner()` methods; `ArrowStore::advance_to_simulation()` is `pub(crate)`. The journal accepts submissions from systems with `JournalSubmitToken` but only the scheduler's commit path holds `CommitStore`.

### I-JRN-HISTORY-BOUND
In-memory commit history (`VecDeque<CommitRecord>`) is bounded at `MAX_COMMIT_HISTORY` (default: 1024). When the history exceeds this bound, the oldest entry is evicted via `pop_front`. This prevents unbounded memory growth during long-running simulations.

### I-JRN-TICK-MONOTONIC
`Tick::next()` saturates at `Tick::MAX` rather than wrapping to 0. `Tick::MAX` is defined as `Tick(u64::MAX - 1)`, with `Tick(u64::MAX)` reserved as a sentinel. This preserves monotonicity under overflow and ensures `Tick::MAX` is a reliable upper bound for all boundary checks.