## Requirements

### Requirement: Snapshot Persistence
The system SHALL save the world state by persisting the `ArrowStore` snapshots and the `CommandJournal`.

#### Scenario: Saving game
- **WHEN** the user saves the game
- **THEN** the system writes the current generation's Arrow batches to disk in IPC format.

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
The save system SHALL maintain a `SaveJournal` (append-only IPC file) that accumulates diffs from each committed tick. Two persistence modes exist:

| Mode | Behavior |
|------|---------|
| **Quick Save** | Appends current tick's diffs to `save_journal.bin`. No snapshot is generated. |
| **Full Save** | Writes a complete `WorldSnapshot` (`snapshot_gen_N.arrow`), then truncates the `SaveJournal`. |

#### Snapshot file structure:
```
+------------------------------+
|  Snapshot Header              |
|  +- SchemaManifest (TOML)     |  <- TableSpec, FieldSemantic, RelationGraph
|  +- ModFingerprint list       |  <- Per-mod: ID, version, TableSpec names, content hash
|  +- Generation number         |
|  +- State hash                |
+------------------------------+
|  Arrow RecordBatches          |  <- Columnar table data
+------------------------------+
```

#### Automatic checkpoint:
- When the SaveJournal contains diffs spanning **1000 ticks** without an intervening full save (i.e., `current_tick - checkpoint_tick >= 1000`), the system automatically triggers a full checkpoint to prevent unbounded journal growth.
- The threshold being configurable (e.g., via a config file) is deferred as future work; the initial implementation uses a hard-coded constant.

#### Load reconstruction -- six-phase lifecycle

Loading a save game follows a strict six-phase sequence. Each phase must complete fully before the next begins:

| Phase | Action | Responsible | Output | Type Authority |
|-------|--------|-------------|--------|----------------|
| 1 -- Snapshot Deserialize | Locate `snapshot_gen_N.arrow`, parse header to extract `SchemaManifest` + `ModFingerprint` list | `save-system` | `SchemaManifest` (saved version) | `scharnhorst_schema::manifest` |
| 2 -- Mod Coordination | Compare stored `ModFingerprint` against currently available mods on disk. Apply tolerance policy (see table below). Produce final mod list. | `save-system` + `content-loader` | Final mod list (for Phase 4) | `scharnhorst_schema::manifest` |
| 3 -- Schema Migration | Run `Migration` functions against `SchemaManifest` from Phase 1 to bring schema to current engine version. | `save-system` + `schema-registry` | `MigratedSchemaManifest` (in memory) | `scharnhorst_schema::manifest` |
| 4 -- Content Compilation | `content-loader` receives `MigratedSchemaManifest` from Phase 3; compiles base + all available mods into Arrow tables; registers `TableSpec`s in `schema-registry`; builds `RelationGraph`. **Global cycle detection**: After all edges are added, the registry MUST perform a cycle detection pass over the entire relation graph using DFS-based topological sort. If a cycle is found, the load SHALL be rejected with a descriptive error including the cycle path. | `content-loader` + `schema-registry` + `arrow-store` | Populated `schema-registry` with `RelationGraph` | `scharnhorst_schema::manifest` |
| 5 -- Schema Freeze | `schema-registry.is_frozen()` is set to `true`. No further `TableSpec` registrations accepted. | `schema-registry` | Frozen registry | -- |
| 6 -- Simulation Start | `sim-scheduler` begins first tick. | `sim-scheduler` | -- | -- |

Detailed step-by-step:
```
1. Locate latest snapshot file: snapshot_gen_N.arrow
2. Parse Snapshot Header -> extract SchemaManifest + ModFingerprint list
3. Compare ModFingerprints against mods on disk (Phase 2)
   - Mod present in save but missing on disk -> warn; mark affected tables "degraded"
   - Mod present on disk but not in save -> include in compilation (Phase 4)
   - Version mismatch -> warn; attempt load with available version
   - Base-game mod missing -> REJECT load
4. Run Migration functions on SchemaManifest (Phase 3) -> produce MigratedSchemaManifest (in memory)
5. Pass MigratedSchemaManifest to content-loader (Phase 4 entry point)
6. content-loader compiles base + mods -> Arrow tables conforming to migrated schema
7. schema-registry registers all TableSpecs, builds RelationGraph
8. Freeze schema-registry (Phase 5)
9. Load save_journal entries with tick > N
10. Replay diffs in order: snapshot_N -> apply(diff_{N+1}) -> ... -> snapshot_M
11. Verify final state hash matches stored hash (corruption detection)
12. Begin simulation (Phase 6)
```

**Migrated Schema Transfer Protocol**:
The `MigratedSchemaManifest` produced in Phase 3 is passed to Phase 4 via shared memory reference:
- Phase 3 completes -> `save-system` holds `MigratedSchemaManifest`
- Phase 4 begins -> `content-loader` receives reference to `MigratedSchemaManifest`
- `content-loader` uses migrated schema as the target schema for compiled content
- `schema-registry` enforces conformance during `TableSpec` registration

### Requirement: Mod-Aware Load Tolerance (content-loader, schema-registry)
When loading a save, the system SHALL compare the stored `ModFingerprint` list against the currently available mods. The following outcomes are defined:

| Scenario | Behavior |
|----------|---------|
| All mods match exactly | Normal load |
| Mod version mismatch | Warn player; attempt load with available version (graceful degradation) |
| Mod present in save but missing on disk | Warn player; mark affected tables as "degraded" -- rules referencing those tables are skipped |
| Mod present on disk but not in save | Normal load -- extra mods apply from cold start |

If any critical (base-game) mod is missing, the load MUST be rejected with a clear error message.

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