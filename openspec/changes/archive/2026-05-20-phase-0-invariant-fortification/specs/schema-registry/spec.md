## MODIFIED Requirements

### Requirement: Cold-Start Registration Contract (content-loader, save-system)
All `TableSpec` registrations, relation declarations, schema versions, storage type registrations that affect Authority decoding, and mod fingerprint registrations MUST be completed before the `sim-scheduler` starts its first tick. The content-loader is the sole author of the schema-registry during startup, coordinated by the six-phase load lifecycle.

**Load lifecycle integration:**

| Phase | Action | Schema-Registry Role | Output |
|-------|--------|----------------------|--------|
| Phase 1 | Snapshot Deserialize | Parse `SchemaManifest` from save header | `SchemaManifest` |
| Phase 2 | Mod Coordination | Store or compare `ModFingerprint` list for load policy | Final mod list |
| Phase 3 | Schema Migration | Apply migration functions to saved schema metadata | `MigratedSchemaManifest` |
| Phase 4 | Content Compilation | Register all table specs, relation edges, schema versions, tier metadata, and mod fingerprints | Populated registry |
| Phase 5 | Schema Freeze | Irreversibly freeze schema-affecting mutation APIs | Frozen registry |
| Phase 6 | Simulation Start | No mutation role | Scheduler begins |

Once the simulation clock starts, `schema-registry.is_frozen()` SHALL return true, and no new `TableSpec`, relation, schema version, storage type metadata, tier metadata, or mod fingerprint registrations are accepted until the next cold start.

#### Scenario: Registry freezes before first tick
- **WHEN** Phase 5 completes
- **THEN** the schema registry is frozen
- **AND** the scheduler may start Phase 6 only after observing the frozen state

#### Scenario: Post-freeze registration rejected
- **WHEN** a system attempts to register a table after simulation starts
- **THEN** the registry returns a descriptive error
- **AND** the registry remains unchanged

### Requirement: Relation Graph Maintenance (content-loader, query-engine)
The schema-registry SHALL maintain a `RelationGraph`, a directed graph where:

- Nodes are registered tables.
- Edges are named relations with source table, source column, target table, target column, and optional target partition metadata.

The `RelationGraph` is built during cold-start content loading from table and relation declarations. After schema freeze, relation graph mutation SHALL be rejected.

The graph is consumed through `query-engine`; direct relation graph access by simulation systems, `rule-ir`, or `bevy-bridge` is forbidden.

#### Scenario: Relation graph query through query engine
- **WHEN** rule-ir executes a scope jump from province to owner actor
- **THEN** the evaluator requests the lookup through query-engine
- **AND** only query-engine consults the registry relation graph

#### Scenario: Post-freeze relation mutation rejected
- **WHEN** runtime code attempts to add a relation edge after freeze
- **THEN** schema-registry returns an error
- **AND** existing relation graph contents remain unchanged

## ADDED Requirements

### Requirement: Irreversible Freeze
Schema freeze SHALL be irreversible within a running engine instance. There SHALL be no public unfreeze operation.

#### Scenario: Attempting to unfreeze schema
- **WHEN** code attempts to re-open the schema registry after Phase 5
- **THEN** no unfreeze API is available or the operation returns an error
- **AND** simulation continues to observe a frozen registry

### Requirement: Tier Metadata Registration
Authority table tier metadata SHALL be registered before schema freeze. Derived and Ephemeral state ownership metadata SHALL be available to tests and documentation, but SHALL NOT make those states part of the schema registry's Authority table set unless explicitly classified as Authority.

#### Scenario: Registering authority tier metadata
- **WHEN** content-loader registers `actor_state`
- **THEN** schema-registry records that the table is Authority state
- **AND** save-system can identify it as persistable

### Requirement: TableSpec Roundtrip Equality
`TableSpec` SHALL implement `PartialEq` such that a serialized-and-deserialized `TableSpec` compares equal to the original. The `column_index` field, which is `#[serde(skip)]` and rebuilt during deserialization, SHALL be excluded from `PartialEq` comparison to ensure roundtrip equality.

#### Scenario: Serialized spec equals deserialized spec
- **WHEN** a `TableSpec` is serialized to JSON and deserialized back
- **THEN** `assert_eq!(original, deserialized)` passes
- **AND** the `column_index` is correctly rebuilt from the `columns` list after deserialization

### Requirement: Migration Path Finding Uses BFS
`Migration::find_migration_path` SHALL use breadth-first search to find the shortest migration path between two schema versions. Implementation SHALL use a queue (`VecDeque`) rather than a stack (`Vec::pop`), ensuring the first discovered path is the shortest in terms of migration steps.

#### Scenario: Shortest path found among multiple routes
- **WHEN** multiple migration paths exist between versions A and D
- **THEN** `find_migration_path` returns the path with the fewest migration steps
- **AND** the path is discovered via BFS queue rather than DFS stack
