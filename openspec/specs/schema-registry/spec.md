## Requirements

### Requirement: Table Specification Management
The system SHALL allow the registration of `TableSpec` objects that define the structure, primary keys, and field semantics of a world table.

#### Scenario: Registering a new table
- **WHEN** a module registers a `TableSpec` for "actor_state" with a primary key "actor_id"
- **THEN** the registry accepts the spec and generates a corresponding Arrow Schema.

### Requirement: Field Semantic Tracking
The system MUST track semantic metadata for fields (e.g., `FixedPoint`, `ForeignKey`, `Tick`) independently of the underlying Arrow data type.

#### Scenario: Retrieving field semantics
- **WHEN** querying the registry for the semantic of "treasury" in "actor_state"
- **THEN** the system returns `FieldSemantic::FixedPoint { scale: 10000 }`.

### Requirement: Schema Versioning
The system SHALL maintain a version number for each table and track migration paths between versions.

#### Scenario: Version mismatch detection
- **WHEN** loading a save with "actor_state" version 1.0 into an engine expecting version 1.1
- **THEN** the system identifies the mismatch and triggers the migration pipeline.

### Requirement: Relation Graph Maintenance (content-loader, query-engine)
The schema-registry SHALL maintain a **`RelationGraph`** -- a directed graph where:
- **Nodes** = registered tables
- **Edges** = named relations (e.g., `OwnerOf`, `LocatedIn`) with metadata:
  - Source table + column (`from_column`, REQUIRED -- used for FK traversal)
  - Target table + column (`to_column`, optional -- used for reverse FK traversal; if absent, falls back to target table's `primary_key_column()`)
  - Target table's partition key (e.g., `region_id`)

**Authority**: The `schema-registry` is the **sole author and maintainer** of the RelationGraph. No other component may modify relation definitions.

The `RelationGraph` is built during content loading (cold start) from `TableSpec` relation declarations (Phase 4 of the load lifecycle). It is consumed exclusively through the `query-engine`:
- **query-engine**: Queries the RelationGraph to optimize SQL JOIN plans and resolve cross-partition lookups
- **rule-ir**: Indirectly via `query-engine.lookup_row()` -- direct access forbidden

**Note**: The `arrow-store` does **not** directly access the RelationGraph. Cross-partition query optimization is handled by the `query-engine` which internally consults the RelationGraph.

#### Scenario: Relation graph query
- **WHEN** rule-ir executes `jump_to(Relation::Owner)` on province P in region_12
- **THEN** the evaluator queries the RelationGraph: `OwnerOf(province -> actor)`, learns actor table is in `global` partition, performs indexed lookup on `actor_id`.

### Requirement: Mod Fingerprint Tracking (content-loader, save-system)

**Type authority**: The canonical `ModFingerprint` type SHALL be defined in `scharnhorst_schema::manifest`. The `FingerprintRegistry` and `FingerprintComparison` utilities in `scharnhorst_content` SHALL operate on this canonical type (not a local redefinition).

The `schema-registry` SHALL maintain a **`ModFingerprint`** for each loaded mod, containing:
- **Mod ID** and **version** (as declared in the mod manifest)
- **Set of `TableSpec` names** registered by this mod
- **Content hash** of the compiled Arrow data for this mod

The `ModFingerprint` list is serialized alongside the schema metadata and included in save-game files (see `save-system`). On load, the `content-loader` compares the stored fingerprints against available mods and reports mismatches.

#### Scenario: Detecting mod mismatch on save load
- **WHEN** loading a save whose `ModFingerprint` lists mod "EuropaBarbarorum v2.1"
- **AND** mod "EuropaBarbarorum v2.1" is not present in the current mod directory
- **THEN** the content-loader flags a mismatch; the `save-system` applies graceful degradation (see `save-system` spec).

### Requirement: Cold-Start Registration Contract (content-loader, save-system)

All `TableSpec` registrations, relation declarations, and schema versions MUST be completed **before** the `sim-scheduler` starts its first tick. The content-loader is the sole author of the schema-registry during startup.

#### Load lifecycle integration:

Schema-registry participates in the six-phase load lifecycle (see `save-system`):

| Phase | Action | Schema-Registry Role | Output |
|-------|--------|---------------------|--------|
| **Phase 1** | Snapshot Deserialize | Parse `SchemaManifest` from save header | `SchemaManifest` (migrated) |
| **Phase 2** | Mod Coordination | Store `ModFingerprint` list for comparison | Final mod list |
| **Phase 3** | Schema Migration | Apply migration functions to loaded `SchemaManifest` -> produce migrated baseline schema | `MigratedSchemaManifest` (in-memory) |
| **Phase 4** | Content Compilation | Receive `MigratedSchemaManifest` from Phase 3; register all `TableSpec`s (base + mods) into registry; build `RelationGraph` from declared relations | Populated `schema-registry` with `RelationGraph` |
| **Phase 5** | Schema Freeze | Set `schema-registry.is_frozen() = true`; reject further registrations | Frozen registry |
| **Phase 6** | Simulation Start | No role (scheduler begins first tick) | -- |

**Phase boundary clarification (Phase 3 vs Phase 4)**:
- **Phase 3 (Migration)**: Operates on the **saved SchemaManifest** only. Migration functions transform old schema versions to the current engine's expected version. Output: `MigratedSchemaManifest` held in memory.
- **Phase 4 (Compilation)**: Operates on **content files** (base game + mods). The `content-loader` receives the `MigratedSchemaManifest` from Phase 3 as the target schema. It compiles content definitions into Arrow tables and registers new `TableSpec`s into the `schema-registry`, which builds the `RelationGraph` from declared relations.

**Migrated Schema Transfer Protocol**:
```
Phase 3 completion:
  save-system applies migrations -> MigratedSchemaManifest (in memory)
  | (passed via shared SchemaRegistry state)
Phase 4 start:
  content-loader receives MigratedSchemaManifest reference
  content-loader compiles base + mods -> Arrow tables
  schema-registry registers TableSpecs, builds RelationGraph
```

**Invariant**: Phase 3 migration **must** complete before Phase 4 compilation begins, ensuring that compiled content conforms to the migrated schema version.

#### Invariant:
Once the simulation clock starts, `schema-registry.is_frozen()` returns `true`, and no new `TableSpec` registrations are accepted until the next cold start.

#### Rationale:
Dynamic schema changes mid-simulation would require journal entries encoding schema mutations, complicating deterministic replay and save/load. This is deferred to a future extension.