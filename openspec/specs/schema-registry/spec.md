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

**Cycle Detection:**
When a relation edge is registered, the schema-registry SHALL perform a DFS-based cycle detection pass over the full relation graph. If the new edge would create a cycle, the registration SHALL be rejected with a descriptive error including the cycle path.

**Cross-Table Validation:**
During `TableSpec` registration, the schema-registry SHALL validate that each relation edge's `from_column` field exists on the source table in the `ArrowSchema`. If the column is absent, registration SHALL fail. If `to_column` is absent, lookup SHALL fall back to the target table's `primary_key_column()`.

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

All `TableSpec` registrations, relation declarations, and schema versions MUST be completed **before** the `sim-scheduler` starts its first tick. The registration sequence is:

1. Engine types (`f32`, `f64`, `bool`, `i8`-`i64`, `u8`-`u64`, `String`, `FixedPoint`) pre-registered by `schema-registry` crate.
2. Optional types pre-registered by `scharnhorst_rules`.
3. Operator schemas registered by `scharnhorst_rules` after TypeId availability.
4. `content-loader` calls `schema-registry` to register the content `TableSpec`s it produces.
5. Tier metadata and mod fingerprints registered by `content-loader` before freeze.
6. `schema-registry.freeze()` called. After this, any further `TableSpec` registrations must be rejected.

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

### Requirement: Irreversible Freeze
`SchemaRegistry::freeze()` SHALL be irreversible for a given registry instance. Once frozen, `is_frozen()` SHALL return `true` for the lifetime of the registry. Calling `freeze()` on an already-frozen registry SHALL return an error rather than silently succeeding.

This matches the `content-loader` invariant: schema registration completes in the Initialization phase before the scheduler starts. No runtime schema changes are allowed.

Attempting to register a `TableSpec` on a frozen registry SHALL return a descriptive error identifying the table name and the frozen state.

#### Scenario: Registration rejected after freeze
- **WHEN** a subsystem attempts to register a `TableSpec` after `freeze()` has been called
- **THEN** the registry returns a descriptive error
- **AND** no schema mutation occurs

#### Scenario: Double freeze returns error
- **WHEN** `freeze()` is called on an already-frozen registry
- **THEN** an error is returned
- **AND** `is_frozen()` continues to return `true`

### Requirement: Tier Metadata Registration
The schema-registry SHALL accept and store tier metadata for each registered table. This metadata classifies tables as Authority, Derived, or Ephemeral.

Tier metadata SHALL be registered by `content-loader` during content compilation (Phase 4), before schema freeze (Phase 5). Once freeze occurs, no further tier registrations SHALL be accepted.

`TierRegistry::register()` SHALL reject duplicate registration: if an entry with the same table name already exists, the call returns an error and the existing entry is preserved.

#### Scenario: Authority table registered with tier metadata
- **WHEN** `content-loader` registers an authority table `actor_state`
- **THEN** tier metadata `(table_id, StateTier::Authority)` is stored
- **AND** `save-system` queries tier metadata to determine which tables to persist

#### Scenario: Duplicate tier registration rejected
- **WHEN** a second registration for the same table name is attempted
- **THEN** `TierRegistry::register()` returns an error
- **AND** the original entry is preserved unchanged

### Requirement: TableSpec Roundtrip Equality
`TableSpec` `PartialEq` SHALL compare all fields that define table identity EXCEPT `column_index` on `column_map` entries. Two `TableSpec` values representing the same logical table — same name, same columns (name, type, semantic), same `primary_key_col` — SHALL compare as equal regardless of internal `column_index` values assigned during construction.

`TableSpec::validate_column_references()` SHALL be called after `PartialEq` comparison to verify that column reference integrity is maintained across roundtrip construction.

#### Scenario: Reconstructed TableSpec equals original
- **WHEN** a `TableSpec` is serialized and deserialized back
- **THEN** the resulting `TableSpec` compares `Eq` to the original
- **AND** the column references are valid for the reconstructed spec

### Requirement: Migration Path Finding Uses BFS
When searching for migration paths between two table schema versions, the system SHALL use BFS (Breadth-First Search) to guarantee the shortest migration path is found. All edges in the migration graph SHALL have uniform weight: each migration step is treated as equal cost.

#### Scenario: Shortest migration path found
- **WHEN** a table has registered migrations `v1->v2`, `v2->v4`, and a direct `v1->v3`
- **AND** the target is `v4`
- **THEN** BFS finds the path `v1->v2->v4` (2 steps) rather than `v1->v3` followed by additional steps
- **AND** if `v1->v4` exists as a direct migration, BFS returns it as the single-step shortest path

---

## Invariants

### I-SCHEMA-FREEZE-IRREVERSIBLE
`SchemaRegistry::freeze()` is irreversible for a given registry instance. Once frozen, `is_frozen()` returns `true` for the lifetime of the registry. Calling `freeze()` on an already-frozen registry returns an error. Registering a `TableSpec` on a frozen registry returns a descriptive error.

### I-SCHEMA-TIER-DEDUP
`TierRegistry::register()` SHALL reject duplicate registration: if an entry with the same table name already exists, the call returns an error and the existing entry is preserved. Silent overwrite does not occur.

### I-SCHEMA-GRAPH-ACYCLIC
When a relation edge is registered, the schema-registry performs DFS-based cycle detection on the full relation graph. An edge that would create a cycle is rejected with a descriptive error including the cycle path. The relation graph is guaranteed acyclic at all times.

### I-SCHEMA-ROUNDTRIP-EQ
`TableSpec` `PartialEq` compares all identity-defining fields EXCEPT `column_index` on `column_map` entries. Two `TableSpec` values representing the same logical table (same name, columns, primary key) SHALL compare equal regardless of internal index values.

### I-SCHEMA-MIGRATION-BFS
Migration path finding uses BFS to guarantee the shortest path between schema versions. All edges have uniform weight. This ensures minimal migration step count when multiple paths exist between source and target versions.