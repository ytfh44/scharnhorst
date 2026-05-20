## Requirements

### Requirement: Snapshot Persistence
The system SHALL save the world state by persisting Authority `ArrowStore` snapshots and the committed journal history needed for reconstruction. Derived and Ephemeral state SHALL be excluded from snapshot files, journal replay data, and deterministic state hashes.

#### Scenario: Saving game
- **WHEN** the user saves the game
- **THEN** the system writes the current generation's Authority Arrow batches to disk in IPC format
- **AND** Derived caches and Ephemeral UI/frame state are not written

#### Scenario: Save excludes derived cache
- **WHEN** a map heatmap Derived cache exists at save time
- **THEN** the save file excludes the cache
- **AND** loading the save rebuilds or invalidates the cache from Authority state

### Requirement: Incremental Save/Load
The system MUST support incremental saves by storing a full snapshot followed by a sequence of diff journals.

#### Scenario: Quick-save
- **WHEN** performing a quick-save
- **THEN** the system only writes the diffs since the last full snapshot.

### Requirement: Schema Migration
The system SHALL apply a sequence of `Migration` functions to update old save data to the current schema version.

#### Scenario: Updating a save
- **WHEN** loading a save from v1.0 into v1.1
- **THEN** the system applies the `v1_0_to_v1_1` migration to the "actor_state" table before starting the simulation.

### Requirement: Checkpoint & Journal Truncation (arrow-store, journal-system)
The save system SHALL maintain a `SaveJournal` that accumulates diffs from each committed tick. Checkpoints and journal replay SHALL operate only on Authority state.

| Mode | Behavior |
|------|----------|
| Quick Save | Appends current tick's committed diffs to `save_journal.bin`. No Derived or Ephemeral state is written. |
| Full Save | Writes a complete Authority `WorldSnapshot`, then truncates the `SaveJournal`. |

`CheckpointingSaveJournal::append()` SHALL check whether the journal entries since the last full checkpoint exceed the auto-checkpoint threshold (default: 1000 ticks). When the threshold is exceeded, the system SHALL enforce snapshot retention (deleting oldest snapshots beyond the configured max). Full snapshot writing and journal truncation are deferred to the `write_checkpoint_snapshot` path because that operation requires access to the current world state (`ArrowStore` / `Journal`). Callers holding world state SHALL periodically invoke `write_checkpoint_snapshot` to prevent unbounded journal growth.

#### Scenario: Auto-checkpoint enforces retention
- **WHEN** the SaveJournal accumulates 1000 entries since the last checkpoint
- **AND** `append()` is called for the 1001st entry
- **THEN** retention enforcement deletes oldest snapshots beyond the configured max
- **AND** the journal continues accumulating entries until a full snapshot is written via `write_checkpoint_snapshot`
- **AND** the journal does not grow unboundedly when snapshots are periodically written

Loading a save game follows a strict six-phase sequence. Each phase must complete fully before the next begins. `LoadReconstruction` SHALL enforce phase ordering: phase N+1 SHALL NOT begin until phase N has completed successfully via `lifecycle.advance()`. Calling phases out of order SHALL return an error.

| Phase | Action | Responsible | Output |
|-------|--------|-------------|--------|
| 1 | Snapshot Deserialize | `save-system` | Saved schema manifest and Authority snapshot |
| 2 | Mod Coordination | `save-system` plus `content-loader` | Final mod list |
| 3 | Schema Migration | `save-system` plus `schema-registry` | Migrated schema manifest |
| 4 | Content Compilation | `content-loader` plus `schema-registry` plus initialization store capability | Authority tables and frozen-ready registry |
| 5 | Schema Freeze | `schema-registry` | Frozen registry |
| 6 | Simulation Start | `sim-scheduler` | Scheduler begins |

After checkpoint Authority state is loaded, post-checkpoint diffs SHALL replay through the journal-owned mutation semantics, not through arbitrary direct store mutation.

#### Scenario: Auto-checkpoint persists authority only
- **WHEN** the SaveJournal reaches the checkpoint threshold
- **THEN** the system writes a full Authority snapshot
- **AND** Derived and Ephemeral state are excluded

#### Scenario: Replay uses journal path
- **WHEN** loading a save with checkpoint N and diffs through tick M
- **THEN** the save-system replays diffs using journal-owned mutation semantics
- **AND** final Authority state hash is verified before simulation resumes

### Requirement: Migrated Schema Transfer Protocol (Phase 3 -> Phase 4)
The `MigratedSchemaManifest` produced in Phase 3 is passed to Phase 4 via shared memory reference:
- Phase 3 completes -> `save-system` holds `MigratedSchemaManifest`
- Phase 4 begins -> `content-loader` receives reference to `MigratedSchemaManifest`
- `content-loader` uses migrated schema as the target schema for compiled content
- `schema-registry` enforces conformance during `TableSpec` registration

### Requirement: Tier-Aware Load Reconstruction
Load reconstruction SHALL restore Authority state from persisted data and SHALL initialize Derived/Ephemeral state through refresh or subsystem initialization rules.

#### Scenario: Loading derived state
- **WHEN** a save is loaded and simulation resumes
- **THEN** Derived caches start empty, dirty, or rebuilt from Authority state
- **AND** no Derived cache is trusted from the save file as proof data

#### Scenario: Loading ephemeral state
- **WHEN** a save is loaded
- **THEN** Ephemeral UI state such as hover, selection, and animation state starts from subsystem defaults
- **AND** it does not affect the restored Authority hash

### Requirement: Mod Tolerance Policy Enforcement
`ModTolerancePolicy::Strict` SHALL reject any mismatch between stored and available mod fingerprints. Attempting to load a save with mismatched or missing mods under `Strict` policy SHALL return an error rather than proceeding with degraded state.

`ModTolerancePolicy::Lenient` SHALL attempt graceful degradation: warn on version mismatch, skip rules referencing tables from missing mods, and include extra mods on disk in the compilation set.

#### Scenario: Strict policy rejects mod mismatch
- **WHEN** a save is loaded with `Strict` tolerance policy
- **AND** one of the stored mods has a different version on disk
- **THEN** the load is rejected with a descriptive error
- **AND** the error identifies which mod and its expected vs actual version

#### Scenario: Lenient policy allows version mismatch
- **WHEN** a save is loaded with `Lenient` tolerance policy
- **AND** a stored mod has a newer version on disk
- **THEN** the load proceeds with a warning
- **AND** the available version is used for compilation

### Requirement: Replay Diff Hash Matches Journal Hash Algorithm
The `replay_diffs` method in load reconstruction SHALL compute the state hash using the same algorithm as `Journal::apply_diffs_and_hash`. The initial hash value SHALL be set to the snapshot's `state_hash` from the checkpoint header. Each replayed diff record's hash SHALL be chained via `wrapping_add` to produce the running hash. The final hash after replaying all diffs SHALL match the saved expected hash exactly.

Both live simulation and replay SHALL use `wrapping_add` semantics for hash chaining, producing a deterministic final hash. The hash contribution of each individual diff record SHALL be identical whether computed during live simulation commitment or during load reconstruction replay.

#### Scenario: Replay hash matches live hash
- **WHEN** the same sequence of diffs is applied during live simulation and during load reconstruction replay
- **THEN** both paths produce the same final state hash
- **AND** the hash verification at load time succeeds

### Requirement: Mod-Aware Load Tolerance (content-loader, schema-registry)
When loading a save, the system SHALL compare the stored `ModFingerprint` list against the currently available mods, applying the selected `ModTolerancePolicy` (see `Mod Tolerance Policy Enforcement` above). Under `Strict`, any mismatch SHALL reject the load. Under `Lenient`, the following outcomes apply:

| Scenario | Behavior (Lenient) |
|----------|--------------------|
| All mods match exactly | Normal load |
| Mod version mismatch | Warn player; attempt load with available version |
| Mod present in save but missing on disk | Warn player; mark affected tables as "degraded" — rules referencing those tables are skipped |
| Mod present on disk but not in save | Normal load — extra mods apply from cold start |

Under either policy, if any critical (base-game) mod is missing, the load MUST be rejected with a clear error message.

### Requirement: Rollback & Historical Snapshots (journal-system)
The system SHALL retain **at most 3 full snapshots total** (including the current one) on disk, enabling rollback to a previous state while bounding disk usage.

#### Scenario: Player rollback
- **WHEN** the player loads from an autosave from 2 hours ago
- **THEN** the system loads snapshot_gen_K.arrow and replays diffs from tick K to the current tick.

#### Snapshot retention:
- The total number of retained snapshots (including the current) SHALL NOT exceed 3.
- On each full save, if the total reaches 4, the oldest snapshot is deleted.
- The SaveJournal is always relative to the most recent retained snapshot.

### Requirement: Multiplayer Save Architecture (bevy-bridge, sim-scheduler)
In multiplayer mode, only the **authoritative server** maintains the SaveJournal. Clients send `InputCommand` messages and reconstruct state by:
1. Loading the shared snapshot
2. Replaying the journal
3. Receiving server-authoritative state syncs

#### Scenario: Client joins mid-game
- **WHEN** a new client connects to an in-progress session
- **THEN** the server sends the latest snapshot + remaining journal diffs; the client replays to reach current state.

---

## Invariants

### I-SAVE-TIER-AWARE
Only Authority state is persisted in save snapshots, checkpoint files, and journal diffs. Derived caches and Ephemeral UI/frame state are excluded from all persistence paths and from deterministic state hashes. The six-phase load lifecycle enforces that Derived/Ephemeral state is rebuilt or default-initialized after Authority state is restored.

### I-SAVE-REPLAY-HASH
`replay_diffs` computes the chain hash using the same `wrapping_add` algorithm as `Journal::apply_diffs_and_hash`. The initial hash SHALL be the snapshot's `state_hash` from the checkpoint header. Each replayed diff's hash chains via `wrapping_add`. The final hash after replaying all diffs SHALL match the saved expected hash exactly. This guarantees live simulation and load reconstruction produce identical hashes for the same diff sequence.

### I-SAVE-LIFECYCLE-ORDER
`LoadReconstruction` enforces strict phase ordering: phase N+1 SHALL NOT begin until phase N has completed successfully via `lifecycle.advance()`. Calling phases out of order returns an error. This prevents content compilation from proceeding before schema migration, and simulation from starting before schema freeze.

### I-SAVE-MOD-STRICT
`ModTolerancePolicy` (see `Mod Tolerance Policy Enforcement` and `Mod-Aware Load Tolerance`) enforces fingerprint-based mod validation on load. Under `Strict`, any mismatch between stored and available fingerprints SHALL reject the load with a descriptive error identifying the mismatched mod and its expected vs actual version. Under `Lenient`, version mismatches produce warnings and the load proceeds with available mod versions; missing mods cause affected tables to be marked degraded and their rules skipped. Under either policy, a missing base-game mod SHALL reject the load.