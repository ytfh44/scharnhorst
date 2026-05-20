//! Integration tests: SchemaRegistry freeze semantics.
//!
//! ## Constraint Map
//!
//! | Constraint | Description |
//! |------------|-------------|
//! | | schema-registry must reject TableSpec registrations after freeze |
//! | | Six-phase load lifecycle: Phase 4 compiles, Phase 5 freezes |
//!
//! ### Phase Boundary
//!
//! ```text
//! Phase 4 (Content Compilation) -> load_from_manifest succeeds
//! Phase 5 (Schema Freeze) -> freeze sets is_frozen = true
//! Phase 6 (Simulation Start) -> all writes rejected, all reads allowed
//! ```

use scharnhorst_schema::{
    ColumnSpec, FieldSemantic, MigratedSchemaManifest, RelationEdge, RelationKind, SchemaError,
    SchemaManifest, SchemaRegistry, SchemaResult, TableSpec,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn make_spec(name: &str) -> TableSpec {
    let col = ColumnSpec::new("id", FieldSemantic::Id, "u64");
    TableSpec::new(name).with_column(col).unwrap_or_else(|_| {
        // with_column only errors on duplicate column names; unreachable for this helper
        TableSpec::new(name)
    })
}

fn make_relation(from: &str, to: &str) -> RelationEdge {
    RelationEdge {
        from: from.to_string(),
        to: to.to_string(),
        kind: RelationKind::OneToMany,
        from_column: "id".to_string(),
        to_column: None,
    }
}

fn make_manifest_with_tables(table_names: &[&str]) -> MigratedSchemaManifest {
    let mut manifest = SchemaManifest::new("1.0.0");
    for name in table_names {
        manifest = manifest.with_table(make_spec(name));
    }
    MigratedSchemaManifest::new(manifest, "0.9.0")
}

// ---------------------------------------------------------------------------
// New registry defaults to NOT frozen
// ---------------------------------------------------------------------------

/// invariant: a freshly-constructed `SchemaRegistry` must not be frozen.
/// This ensures that Phases 1-4 can populate the registry before Phase 5 freezes it.
#[test]
fn dc5_new_registry_defaults_to_not_frozen() {
    let registry = SchemaRegistry::new();
    assert!(
        !registry.is_frozen(),
        "DC-5: new registry must default to unfrozen so Phase 1-4 can register tables"
    );
}

// ---------------------------------------------------------------------------
// Phase 5: freeze sets is_frozen = true
// ---------------------------------------------------------------------------

/// Phase 5: calling `freeze` must transition the registry to the frozen state.
#[test]
fn dc13_phase5_freeze_sets_is_frozen_true() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    assert!(
        !registry.is_frozen(),
        "precondition: unfrozen before Phase 5"
    );
    registry.freeze();
    assert!(
        registry.is_frozen(),
        "DC-13 Phase 5: freeze() must set is_frozen() = true"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// / Phase 5: frozen registry rejects write operations
// ---------------------------------------------------------------------------

/// frozen registry must reject `register` with `SchemaError::RegistryFrozen`.
#[test]
fn dc5_frozen_registry_rejects_register() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.freeze();

    let result = registry.register(make_spec("Unit"));
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: frozen registry must reject register() with RegistryFrozen"
    );
    Ok(())
}

/// frozen registry must reject `add_relation` with `SchemaError::RegistryFrozen`.
#[test]
fn dc5_frozen_registry_rejects_add_relation() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();

    // Pre-freeze: register tables so the relation would otherwise be valid.
    registry.register(make_spec("A"))?;
    registry.register(make_spec("B"))?;
    registry.freeze();

    let result = registry.add_relation(make_relation("A", "B"));
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: frozen registry must reject add_relation() with RegistryFrozen"
    );
    Ok(())
}

/// frozen registry must reject `get_mut` with `SchemaError::RegistryFrozen`.
#[test]
fn dc5_frozen_registry_rejects_get_mut() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.register(make_spec("Unit"))?;
    registry.freeze();

    let result = registry.get_mut("Unit");
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: frozen registry must reject get_mut() with RegistryFrozen"
    );
    Ok(())
}

/// frozen registry must reject `remove` with `SchemaError::RegistryFrozen`.
#[test]
fn dc5_frozen_registry_rejects_remove() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.register(make_spec("Unit"))?;
    registry.freeze();

    let result = registry.remove("Unit");
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: frozen registry must reject remove() with RegistryFrozen"
    );
    Ok(())
}

/// frozen registry must reject `remove_relation` with `SchemaError::RegistryFrozen`.
#[test]
fn dc5_frozen_registry_rejects_remove_relation() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.register(make_spec("A"))?;
    registry.register(make_spec("B"))?;
    registry.add_relation(make_relation("A", "B"))?;
    registry.freeze();

    let result = registry.remove_relation("A", "B");
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: frozen registry must reject remove_relation() with RegistryFrozen"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 6: frozen registry still allows read operations
// ---------------------------------------------------------------------------

/// Phase 6: after freeze (Phase 5), all read operations remain available.
/// The simulation can query the schema but cannot mutate it.
#[test]
fn dc13_phase6_frozen_registry_allows_read_operations() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();

    // Phase 4: populate
    registry.register(make_spec("A"))?;
    registry.register(make_spec("B"))?;
    registry.add_relation(make_relation("A", "B"))?;

    // Phase 5: freeze
    registry.freeze();

    // Phase 6: all reads must succeed
    assert!(
        registry.contains("A"),
        "DC-13 Phase 6: contains() must work on frozen registry"
    );
    assert!(
        registry.contains("B"),
        "DC-13 Phase 6: contains() must work on frozen registry"
    );
    assert_eq!(
        registry.table_count(),
        2,
        "DC-13 Phase 6: table_count() must work on frozen registry"
    );

    let mut names: Vec<&str> = registry.table_names().collect();
    names.sort();
    assert_eq!(
        names,
        vec!["A", "B"],
        "DC-13 Phase 6: table_names() must work on frozen registry"
    );

    let spec_a = registry.get("A")?;
    assert_eq!(
        spec_a.name, "A",
        "DC-13 Phase 6: get() must work on frozen registry"
    );

    let children: Vec<&str> = registry.children_of("A").collect();
    assert_eq!(
        children,
        vec!["B"],
        "DC-13 Phase 6: children_of() must work on frozen registry"
    );

    // relation_graph returns the read-only reference
    let graph = registry.relation_graph();
    let all_edges = graph.all_edges();
    assert_eq!(
        all_edges.len(),
        1,
        "DC-13 Phase 6: relation_graph() must work on frozen registry"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Idempotency: freeze is idempotent
// ---------------------------------------------------------------------------

/// Phase 5: calling `freeze` on an already-frozen registry is idempotent.
/// The registry must remain frozen after repeated freeze calls.
#[test]
fn dc13_phase5_freeze_is_idempotent() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.freeze();
    assert!(registry.is_frozen(), "first freeze");
    registry.freeze();
    assert!(
        registry.is_frozen(),
        "DC-13 Phase 5: freeze() must be idempotent, registry stays frozen"
    );
    registry.freeze();
    assert!(
        registry.is_frozen(),
        "DC-13 Phase 5: triple freeze() still frozen"
    );
    Ok(())
}

/// idempotent freeze still rejects writes.
#[test]
fn dc5_idempotent_freeze_still_rejects_writes() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    registry.freeze();
    registry.freeze(); // idempotent
    let result = registry.register(make_spec("StillFrozen"));
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-5: after idempotent freeze(), register() must still be rejected"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 4 -> Phase 5 boundary: load_from_manifest
// ---------------------------------------------------------------------------

/// Phase 4: `load_from_manifest` succeeds before freeze.
/// This is the normal path: Phase 3 migrates, Phase 4 compiles into the registry.
#[test]
fn dc13_phase4_load_from_manifest_succeeds_before_freeze() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();
    let migrated = make_manifest_with_tables(&["A", "B", "C"]);

    // Phase 4: load the migrated manifest into the registry
    registry.load_from_manifest(&migrated)?;

    assert_eq!(
        registry.table_count(),
        3,
        "DC-13 Phase 4: load_from_manifest should register all tables from the manifest"
    );
    assert!(registry.contains("A"));
    assert!(registry.contains("B"));
    assert!(registry.contains("C"));
    Ok(())
}

/// Phase 5 boundary: `load_from_manifest` must fail after freeze.
/// Once Phase 5 freezes the registry, no further bulk-loads are permitted.
#[test]
fn dc13_phase5_load_from_manifest_fails_after_freeze() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();

    // Phase 4: initial load
    let migrated1 = make_manifest_with_tables(&["A"]);
    registry.load_from_manifest(&migrated1)?;
    assert!(registry.contains("A"));

    // Phase 5: freeze
    registry.freeze();
    assert!(
        registry.is_frozen(),
        "DC-13 Phase 5: is_frozen() returns true before Phase 6 Simulation Start"
    );

    // Attempt another load_from_manifest after freeze -- must fail
    let migrated2 = make_manifest_with_tables(&["X"]);
    let result = registry.load_from_manifest(&migrated2);
    assert_eq!(
        result,
        Err(SchemaError::RegistryFrozen),
        "DC-13 Phase 4->Phase 5 boundary: load_from_manifest() must fail after freeze"
    );

    // Verify the registry was not mutated
    assert!(
        !registry.contains("X"),
        "DC-13: registry must not be mutated by a failed load_from_manifest after freeze"
    );
    assert_eq!(
        registry.table_count(),
        1,
        "DC-13: table count must be unchanged after rejected load_from_manifest"
    );

    Ok(())
}

/// Phase 5: `load_from_manifest` with relations works before freeze.
/// Verifies the full manifest load (tables + relations) during Phase 4.
#[test]
fn dc13_phase4_load_from_manifest_with_relations() -> SchemaResult<()> {
    let mut registry = SchemaRegistry::new();

    let manifest = SchemaManifest::new("1.0.0")
        .with_table(make_spec("A"))
        .with_table(make_spec("B"))
        .with_relation(make_relation("A", "B"));
    let migrated = MigratedSchemaManifest::new(manifest, "0.9.0");

    // Phase 4: load with tables and relations
    registry.load_from_manifest(&migrated)?;

    assert_eq!(registry.table_count(), 2);
    let children: Vec<&str> = registry.children_of("A").collect();
    assert_eq!(
        children,
        vec!["B"],
        "DC-13 Phase 4: relations from manifest must be registered"
    );

    Ok(())
}
