## MODIFIED Requirements

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

## ADDED Requirements

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

### Requirement: Migration Registry Clone Preserves Steps
`MigrationRegistry` SHALL support cloning so that downstream consumers can snapshot the current migration state.

> **Deferred**: `MigrationRegistry::Clone` is deferred because `Migration::apply_fn` uses `Box<dyn Fn>` which is fundamentally incompatible with `#[derive(Clone)]`.
> Tracked in tasks.md § 15.10.8.1. A future workaround may use `Arc<dyn Fn>` or `fn` pointer replacement.
> The test `migration_registry_preserves_state_for_consumers` in `schema_tests.rs` documents the expected behavior.

#### Scenario: Cloned registry applies same migrations (DEFERRED)
- **WHEN** a `MigrationRegistry` with three steps is cloned (once Clone is implemented)
- **THEN** the clone has the same three steps and `target_version`
- **AND** `clone.apply(manifest)` produces the same result as `original.apply(manifest)`

### Requirement: Replay Diff Hash Matches Journal Hash Algorithm
The `replay_diffs` method in load reconstruction SHALL compute the state hash using the same algorithm as `Journal::apply_diffs_and_hash`. The initial hash value SHALL be set to the snapshot's `state_hash` from the checkpoint header. Each replayed diff record's hash SHALL be chained via `wrapping_add` to produce the running hash. The final hash after replaying all diffs SHALL match the saved expected hash exactly.

Both live simulation and replay SHALL use `wrapping_add` semantics for hash chaining, producing a deterministic final hash. The hash contribution of each individual diff record SHALL be identical whether computed during live simulation commitment or during load reconstruction replay.

#### Scenario: Replay hash matches live hash
- **WHEN** the same sequence of diffs is applied during live simulation and during load reconstruction replay
- **THEN** both paths produce the same final state hash
- **AND** the hash verification at load time succeeds
