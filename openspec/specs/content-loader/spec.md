## Requirements

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
The system SHALL compile raw text definitions into binary Authority Arrow tables for fast startup. Compilation happens during initialization and MUST NOT create Derived or Ephemeral state inside `ArrowStore`.

The `CompilationOutput` struct SHALL include:
1. Authority Arrow table data for all compiled tables.
2. `TableSpec` registrations for all compiled tables.
3. Authority tier metadata for persisted tables (via `StateTier::Authority` assignments).
4. `RelationGraph` edges derived from declared relations in content definitions.
5. `ModFingerprint` entries for all mods participating in the compilation.

The `CompilationInput` struct SHALL accept a `MigratedSchemaManifest` reference from Phase 3 to validate that compiled table schemas conform to the migrated schema. Compilation SHALL reject tables whose schema does not match the migrated manifest.

#### Scenario: Fast load
- **WHEN** starting the game
- **THEN** the engine may load precompiled Authority table data instead of parsing thousands of TOML files
- **AND** the loaded tables still pass schema, relation, tier, and fingerprint validation before freeze

#### Scenario: Derived content output stays outside Arrow authority
- **WHEN** content compilation computes a prebuilt lookup cache
- **THEN** the cache is classified as Derived
- **AND** it is stored outside Authority tables or rebuilt later from Authority content

### Requirement: Static Merge Strategy (arrow-store, schema-registry, save-system)
Content compilation and mod overlay merging SHALL complete before the `sim-scheduler` starts the first tick. The loader operates as part of the six-phase load lifecycle.

Phase 4 output SHALL include:

1. Compiled Authority Arrow tables written through initialization capabilities.
2. `TableSpec` registrations written to `schema-registry`.
3. Authority tier metadata for persisted tables.
4. `RelationGraph` edges derived from declared relations.
5. `ModFingerprint` entries registered before schema freeze.

No content changes occur at runtime. Mod updates require a restart unless a future hot-reload capability defines journal, replay, and scheduler semantics.

#### Scenario: Loading with mods active
- **WHEN** the game starts with three mods installed
- **THEN** the loader receives the final mod list from Phase 2
- **AND** it applies mod overlays in declared priority order
- **AND** it compiles merged Authority content into Arrow tables through initialization capabilities
- **AND** it registers schemas, relations, tier metadata, and fingerprints before schema freeze

#### Scenario: Runtime content change rejected
- **WHEN** a mod file changes while simulation is running
- **THEN** the content-loader does not hot-reload it
- **AND** no Authority table or schema registry mutation occurs

### Requirement: Mod Fingerprint Registration (schema-registry, save-system)

**Type authority**: The content-loader SHALL use the canonical `scharnhorst_schema::manifest::ModFingerprint` type when generating and registering fingerprints. Local redefinition is FORBIDDEN. The `FingerprintRegistry` and `FingerprintComparison` utility types SHALL remain in `scharnhorst_content` but operate on the canonical type from `scharnhorst_schema`.

The content-loader SHALL generate a `ModFingerprint` for each loaded mod and register it in the `schema-registry`. The fingerprint includes the mod ID, version, set of registered `TableSpec` names, and the content hash of the compiled Arrow data.

The combined fingerprint list is embedded in the output snapshot files (see `save-system`), enabling mismatch detection on load.

**Lifecycle note**: Mod fingerprint comparison (Phase 2 of the load lifecycle, see `save-system`) determines which mods are available for compilation. The content-loader receives the final mod list after Phase 2 completes. Mods present on disk but absent from the save ARE included in the compilation output and their `ModFingerprint` is registered normally.

#### Scenario: Embedding fingerprints during cold start
- **WHEN** the game starts with mods "EuropaBarbarorum v2.1" and "RiseOfRome v1.0"
- **THEN** the content-loader registers two `ModFingerprint` entries in the `schema-registry`. When a snapshot is persisted, these fingerprints are stored in the snapshot header.

#### Migrated Schema Transfer (Phase 3 -> Phase 4)

The content-loader receives the `MigratedSchemaManifest` from Phase 3 as follows:
1. Phase 3 completes: `save-system` applies migration functions to the saved `SchemaManifest`, producing `MigratedSchemaManifest` (held in memory)
2. Phase 4 begins: `content-loader` is given a reference to the `MigratedSchemaManifest`
3. `content-loader` compiles base + mods into Arrow tables that conform to the migrated schema
4. `schema-registry` registers all `TableSpec`s and builds the `RelationGraph`

**Invariant**: The migrated schema from Phase 3 serves as the target schema that compiled content must conform to. This is enforced by the `schema-registry` during `TableSpec` registration.

### Requirement: Schema Metadata Embedding (save-system, schema-registry)

**Type authority**: The content-loader SHALL use the canonical `scharnhorst_schema::manifest::SchemaManifest` type. The `MigratedSchemaManifest` type is also defined in `scharnhorst_schema::manifest` -- the content-loader accesses `.tables()` and `.relations()` via accessor methods on the canonical type.

The content-loader SHALL serialize a `SchemaManifest` -- containing all `TableSpec` definitions, field semantics, `RelationGraph` edges, and `ModFingerprint` entries -- into the snapshot file header. This makes every saved game **self-contained**: a client receiving only the snapshot file can reconstruct the full `schema-registry` without external configuration.

**Serialization Source**: The `SchemaManifest` is extracted from the `schema-registry` after Phase 4 (Content Compilation) completes and before Phase 5 (Schema Freeze). At this point, the registry contains:
- All `TableSpec` definitions (base + mods)
- Field semantic metadata
- Complete `RelationGraph` (all edges)
- `ModFingerprint` list

The `save-system` serializes this manifest into the snapshot header during Phase 6 (Simulation Start) when a full save is triggered.

#### Rationale:
This satisfies the multiplayer bootstrap scenario where a joining client needs the schema to deserialize world state. By embedding the manifest in the snapshot, we avoid a separate schema-sync network round-trip.

### Requirement: No Runtime Hot-Reload
Runtime mod hot-reloading is **explicitly out of scope** for this design. If hot-reload is considered in the future, it must be implemented as a new capability that defines its own interaction with `journal-system` (replay safety) and `sim-scheduler` (pause semantics).

#### Rationale:
Allowing mid-simulation content changes would require: (a) encoding content changes as journal diffs, (b) ensuring deterministic replay across all peers, and (c) handling schema migrations mid-game. These complexities are deferred.

### Requirement: Initialization Capability Use
The content-loader SHALL write schema and Authority table data only through Initialization phase capabilities. It MUST NOT retain those capabilities after Phase 5 schema freeze.

#### Scenario: Loader drops initialization capability
- **WHEN** content compilation finishes and schema freeze begins
- **THEN** content-loader releases or consumes its initialization mutation capability
- **AND** it cannot create or modify Authority tables during simulation

### Requirement: Tier Validation During Compilation
Content compilation SHALL reject or report definitions that attempt to persist Derived or Ephemeral state as Authority tables without explicit Authority classification.

> **Deferred**: Tier validation during compilation is not implemented in Phase 0.
> Tracked in tasks.md section 15.8.3. ALL compiled tables currently default to `StateTier::Authority`.
> The `#[ignore]` test `tier_validation_rejects_non_authority_table_compilation` in `content_tests.rs` documents the expected future behavior.

#### Scenario: Mod defines transient UI table
- **WHEN** a mod declares a table intended to store selected UI panels
- **THEN** compilation rejects the table as non-Authority content
- **AND** the error explains that UI selection is Ephemeral state

### Requirement: Fingerprint Hash Covers Table Specs
Mod fingerprint content hashes SHALL include the set of registered `TableSpec` names and their schema metadata. Two mods with identical `mod_id` and `version` but different table specifications SHALL produce different content hashes.

#### Scenario: Different table specs produce different hashes
- **WHEN** two mods have the same `mod_id` and `version` but define different tables
- **THEN** their `ModFingerprint.content_hash` values differ
- **AND** fingerprint comparison detects the mismatch