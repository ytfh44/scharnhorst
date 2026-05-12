**Crate**: `scharnhorst_content`

## ADDED Requirements

### Requirement: Overlay-Based Loading
The system SHALL load content from a base package and apply mod overlays using a merge/replace strategy. This process occurs **exclusively during cold start** (before the simulation clock begins).

#### Scenario: Modding a value
- **WHEN** a mod defines a new "stability" value for a country that already exists in the base game
- **THEN** the loader replaces the base value with the modded value in the final compiled content.

### Requirement: Name Resolution
The system MUST resolve string identifiers (e.g., "FRA" for France) into stable integer IDs during the loading phase.

#### Scenario: Resolving actor ID
- **WHEN** a rule references actor "FRA"
- **THEN** the content loader maps "FRA" to `ActorId(12)` in the internal registry.

### Requirement: Content Compilation
The system SHALL compile raw text definitions into binary Arrow tables for fast startup.

#### Scenario: Fast load
- **WHEN** starting the game
- **THEN** the engine loads `definitions.arrow` instead of parsing thousands of TOML files.

## MODIFIED Requirements

### (Compatibility) Requirement: Static Merge Strategy (↔ arrow-store, ↔ schema-registry, ↔ save-system)
Content compilation and mod overlay merging SHALL complete **before** the `sim-scheduler` starts the first tick. The loader operates as part of the six-phase load lifecycle (see `save-system` DC-13).

**Phase 4 Output** (Content Compilation):
1. Compiled Arrow tables, written to the `ArrowStore`
2. `TableSpec` registrations, written to the `schema-registry`
3. `RelationGraph` edges derived from declared relations, built by `schema-registry`
4. `ModFingerprint` entries, registered in `schema-registry`

No content changes occur at runtime. Mod updates require a restart.

#### Scenario: Loading with mods active
- **WHEN** the game starts with 3 mods installed
- **THEN** the loader (a) receives the final mod list from Phase 2 (Mod Coordination), (b) reads all base definitions, (c) applies mod overlays in declared priority order, (d) compiles the merged result into Arrow tables, and (e) registers all schemas and relations in `schema-registry`. Only then does the simulation begin.

### (Compatibility) Requirement: Mod Fingerprint Registration (↔ schema-registry, ↔ save-system)

**Type authority**: The content-loader SHALL use the canonical `scharnhorst_schema::manifest::ModFingerprint` type when generating and registering fingerprints. Local redefinition is FORBIDDEN. The `FingerprintRegistry` and `FingerprintComparison` utility types SHALL remain in `scharnhorst_content` but operate on the canonical type from `scharnhorst_schema`.

The content-loader SHALL generate a `ModFingerprint` for each loaded mod and register it in the `schema-registry`. The fingerprint includes the mod ID, version, set of registered `TableSpec` names, and the content hash of the compiled Arrow data.

The combined fingerprint list is embedded in the output snapshot files (see `save-system` Compatibility Requirement), enabling mismatch detection on load.

**Lifecycle note**: Mod fingerprint comparison (Phase 2 of the load lifecycle, see `save-system`) determines which mods are available for compilation. The content-loader receives the final mod list after Phase 2 completes. Mods present on disk but absent from the save ARE included in the compilation output and their `ModFingerprint` is registered normally.

#### Scenario: Embedding fingerprints during cold start
- **WHEN** the game starts with mods "EuropaBarbarorum v2.1" and "RiseOfRome v1.0"
- **THEN** the content-loader registers two `ModFingerprint` entries in the `schema-registry`. When a snapshot is persisted, these fingerprints are stored in the snapshot header.

#### Migrated Schema Transfer (Phase 3 → Phase 4)

The content-loader receives the `MigratedSchemaManifest` from Phase 3 as follows:
1. Phase 3 completes: `save-system` applies migration functions to the saved `SchemaManifest`, producing `MigratedSchemaManifest` (held in memory)
2. Phase 4 begins: `content-loader` is given a reference to the `MigratedSchemaManifest`
3. `content-loader` compiles base + mods into Arrow tables that conform to the migrated schema
4. `schema-registry` registers all `TableSpec`s and builds the `RelationGraph`

**Invariant**: The migrated schema from Phase 3 serves as the target schema that compiled content must conform to. This is enforced by the `schema-registry` during `TableSpec` registration.

### (Compatibility) Requirement: Schema Metadata Embedding (↔ save-system, ↔ schema-registry)

**Type authority**: The content-loader SHALL use the canonical `scharnhorst_schema::manifest::SchemaManifest` type. The `MigratedSchemaManifest` type is also defined in `scharnhorst_schema::manifest` — the content-loader accesses `.tables()` and `.relations()` via accessor methods on the canonical type.

The content-loader SHALL serialize a `SchemaManifest` — containing all `TableSpec` definitions, field semantics, `RelationGraph` edges, and `ModFingerprint` entries — into the snapshot file header. This makes every saved game **self-contained**: a client receiving only the snapshot file can reconstruct the full `schema-registry` without external configuration.

**Serialization Source**: The `SchemaManifest` is extracted from the `schema-registry` after Phase 4 (Content Compilation) completes and before Phase 5 (Schema Freeze). At this point, the registry contains:
- All `TableSpec` definitions (base + mods)
- Field semantic metadata
- Complete `RelationGraph` (all edges)
- `ModFingerprint` list

The `save-system` serializes this manifest into the snapshot header during Phase 6 (Simulation Start) when a full save is triggered.

#### Rationale:
This satisfies the multiplayer bootstrap scenario (DC-9) where a joining client needs the schema to deserialize world state. By embedding the manifest in the snapshot, we avoid a separate schema-sync network round-trip.

### (Compatibility) Requirement: No Runtime Hot-Reload
Runtime mod hot-reloading is **explicitly out of scope** for this design. If hot-reload is considered in the future, it must be implemented as a new capability that defines its own interaction with `journal-system` (replay safety) and `sim-scheduler` (pause semantics).

#### Rationale:
Allowing mid-simulation content changes would require: (a) encoding content changes as journal diffs, (b) ensuring deterministic replay across all peers, and (c) handling schema migrations mid-game. These complexities are deferred.
