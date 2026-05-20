## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Initialization Capability Use
The content-loader SHALL write schema and Authority table data only through Initialization phase capabilities. It MUST NOT retain those capabilities after Phase 5 schema freeze.

#### Scenario: Loader drops initialization capability
- **WHEN** content compilation finishes and schema freeze begins
- **THEN** content-loader releases or consumes its initialization mutation capability
- **AND** it cannot create or modify Authority tables during simulation

### Requirement: Tier Validation During Compilation
Content compilation SHALL reject or report definitions that attempt to persist Derived or Ephemeral state as Authority tables without explicit Authority classification.

> **Deferred**: Tier validation during compilation is not implemented in Phase 0.
> Tracked in tasks.md §15.8.3. ALL compiled tables currently default to `StateTier::Authority`.
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
