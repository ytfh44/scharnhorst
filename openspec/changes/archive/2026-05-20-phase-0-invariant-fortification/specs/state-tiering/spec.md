## ADDED Requirements

### Requirement: World State Tier Taxonomy
The system SHALL classify all runtime state into exactly one of three tiers: Authority, Derived, or Ephemeral.

Authority state is the persisted, replay-authoritative world ledger. It SHALL live in `ArrowStore` as versioned Arrow-backed tables at tick boundaries, participate in save/load, and participate in deterministic state hashing.

Derived state is rebuildable from Authority state plus schema/content inputs. It SHALL NOT be persisted as proof data and SHALL NOT be treated as an independent source of world truth.

Ephemeral state is frame-local, UI-local, command-staging, animation, transient AI planning, or other runtime-only state. It SHALL NOT be stored in `ArrowStore`, persisted in save files, replayed as authoritative state, or included in deterministic state hashes.

#### Scenario: Classifying a new table
- **WHEN** a developer adds a new world table for actor treasury balances
- **THEN** the table is classified as Authority because it persists across ticks and affects replay hashes
- **AND** the table is eligible for `ArrowStore`, save snapshots, and journal diffs

#### Scenario: Rejecting cache state as authority
- **WHEN** a developer attempts to store a map-mode heatmap cache in an Arrow authority table
- **THEN** the system rejects or flags the table classification because the cache is Derived state
- **AND** the cache must be rebuilt from Authority state instead of persisted as ledger state

### Requirement: Authority State Persistence and Hash Participation
Every byte of Authority state SHALL be persisted by the save system. The content hash of every snapshot SHALL depend only on Authority state; Derived and Ephemeral state SHALL NOT participate in the hash. This supports save-game proofing: two loads replaying identical input commands must produce identical Authority hashes even if Derived rebuild ran non-deterministically or Ephemeral animations differed.

Authority state values using `FixedPoint` SHALL compare correctly across different scales. The `Ord` implementation for `FixedPoint` SHALL compare the mathematical values (e.g., `FixedPoint(100, 2)` = 1.00 vs `FixedPoint(200, 3)` = 0.200) rather than comparing `(raw, scale)` as a lexical tuple. For equality comparison, `FixedPoint(1, 0)` SHALL equal `FixedPoint(10, 1)` (both represent 1.0). For ordering, `FixedPoint(100, 2)` (1.00) SHALL be greater than `FixedPoint(200, 3)` (0.200).

`FixedPoint::rescale` SHALL use banker's rounding (round half to even) rather than truncation to minimize cumulative precision loss across multiple rescale operations.

#### Scenario: FixedPoint cross-scale comparison
- **WHEN** comparing `FixedPoint(100, 2)` (1.00) with `FixedPoint(200, 3)` (0.200)
- **THEN** `FixedPoint(100, 2) > FixedPoint(200, 3)` returns `true`
- **AND** `FixedPoint(200, 3) < FixedPoint(100, 2)` returns `true`

#### Scenario: FixedPoint cross-scale equality
- **WHEN** comparing `FixedPoint(10, 0)` (10) with `FixedPoint(100, 1)` (10.0)
- **THEN** both comparisons return `Equal`
- **AND** both represent the same mathematical value 10.0

#### Scenario: Saving authority state
- **WHEN** a full save is produced at tick T
- **THEN** all Authority tables needed to reconstruct snapshot T are written to the save file
- **AND** Derived and Ephemeral state are excluded from the save file
- **AND** the state hash is computed from Authority state and committed diffs only

#### Scenario: Replaying authority diffs
- **WHEN** loading a checkpoint followed by journal diffs
- **THEN** each replayed diff mutates only Authority state
- **AND** replayed state produces the same final hash as the original simulation run

### Requirement: Derived State Rebuild Contract
Derived state SHALL declare how it is rebuilt or invalidated from a read-only world view. A Derived state object MUST NOT mutate Authority state while rebuilding.

Derived rebuilds MAY be eager during refresh handling or lazy on first access after being marked dirty. In both cases, the input SHALL be the current query-engine snapshot or typed query APIs for the committed generation.

#### Scenario: Refresh invalidates derived state
- **WHEN** `journal.commit()` publishes a new generation and the scheduler broadcasts `REFRESH_SIGNAL`
- **THEN** registered Derived state handlers mark their cached data dirty or rebuild from the latest query-engine snapshot
- **AND** no Derived rebuild directly writes to `ArrowStore`

#### Scenario: Lazy derived rebuild
- **WHEN** a map-mode cache is marked dirty after a refresh signal
- **AND** the UI requests that map mode later in the frame
- **THEN** the cache rebuilds from query-engine reads for the latest generation
- **AND** the rebuilt cache remains outside Authority snapshots and replay hashes

### Requirement: Ephemeral State Exclusion
Ephemeral state SHALL be owned by runtime subsystems such as Bevy ECS, UI input buffers, frame-local queues, animation state, temporary pathfinding queues, or AI planning scratch data.

Ephemeral state MUST NOT be written to `ArrowStore`, MUST NOT be serialized in save snapshots, MUST NOT appear in journal diffs, and MUST NOT affect deterministic replay except through explicit commands or diffs submitted to the journal.

#### Scenario: UI hover state remains ephemeral
- **WHEN** the player hovers over a province in the UI
- **THEN** the hover selection is stored in Bevy/UI Ephemeral state
- **AND** no journal diff or Authority table update is produced

#### Scenario: Player command exits ephemeral state
- **WHEN** the player clicks a button that produces a valid gameplay command
- **THEN** the UI event is converted into a command and submitted through the tick-aligned command buffer
- **AND** only the resulting journal-committed diff can affect Authority state

### Requirement: State Tier Registry and Auditability
The system SHALL provide an auditable registry or equivalent metadata source that records which tables and runtime state holders belong to each tier.

Authority table classification SHALL be available to `save-system`, `journal-system`, `query-engine`, and tests. Derived and Ephemeral classification SHALL be documented sufficiently for future domain modules to choose the correct storage location.

`TierRegistry::register()` SHALL reject duplicate registration by returning an error when an entry with the same table name already exists. Silent overwrite of an existing tier entry SHALL NOT occur. The existing entry SHALL be preserved and a descriptive error SHALL be returned to the caller.

#### Scenario: Auditing authority tables
- **WHEN** an integration test enumerates all persisted Arrow tables
- **THEN** every table has an Authority classification
- **AND** the test fails if a persisted table is missing tier metadata

#### Scenario: Auditing non-authority state
- **WHEN** a subsystem registers a Derived cache or Ephemeral store
- **THEN** its tier, owner subsystem, rebuild or discard rule, and persistence policy are documented or registered
- **AND** save/load code ignores that non-authority state for proof reconstruction

#### Scenario: Duplicate tier registration detected
- **WHEN** two subsystems attempt to register tier metadata for the same table name
- **THEN** the second registration returns a descriptive error and the existing entry is preserved
- **AND** the conflicting subsystem receives the error identifying the table name and existing tier
