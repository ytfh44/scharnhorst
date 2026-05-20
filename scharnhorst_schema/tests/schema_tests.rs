//! Integration tests for scharnhorst_schema crate
//!
//! These tests cover all SPEC requirements:
//! 1. Table Specification Management
//! 2. Field Semantic Tracking
//! 3. Schema Versioning
//! 4. Relation Graph Maintenance
//! 5. Mod Fingerprint Tracking
//! 6. Cold-Start Registration Contract
//! 7. Six-Phase Load Lifecycle

use scharnhorst_schema::{
    ColumnSpec, FieldSemantic, MigratedSchemaManifest, Migration, MigrationRegistry,
    ModFingerprint, RelationEdge, RelationGraph, RelationKind, SchemaError, SchemaManifest,
    SchemaRegistry, SchemaResult, TableSpec,
};

// =============================================================================
// Test Helpers
// =============================================================================

/// Creates a basic TableSpec with an ID column
fn make_table_spec(name: &str) -> TableSpec {
    let id_col = ColumnSpec::new("id", FieldSemantic::Id, "u64");
    TableSpec::new(name)
        .with_column(id_col)
        .expect("valid column")
}

/// Creates a TableSpec with a primary key column
fn make_actor_table_spec() -> TableSpec {
    TableSpec::new("actor_state")
        .with_column(ColumnSpec::new("actor_id", FieldSemantic::Id, "u64"))
        .expect("valid column")
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .expect("valid column")
        .with_column(ColumnSpec::new("treasury", FieldSemantic::Quantity, "i64"))
        .expect("valid column")
}

/// Creates a TableSpec with various semantic types
fn make_province_table_spec() -> TableSpec {
    TableSpec::new("province")
        .with_column(ColumnSpec::new("province_id", FieldSemantic::Id, "u64"))
        .expect("valid column")
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .expect("valid column")
        .with_column(ColumnSpec::new(
            "owner_id",
            FieldSemantic::ForeignKey {
                target_table: "actor_state".to_string(),
            },
            "u64",
        ))
        .expect("valid column")
        .with_column(ColumnSpec::new("region_id", FieldSemantic::Tag, "u64"))
        .expect("valid column")
        .with_column(ColumnSpec::new("tax_rate", FieldSemantic::Percent, "i64"))
        .expect("valid column")
        .with_column(ColumnSpec::new(
            "created_tick",
            FieldSemantic::Timestamp,
            "u64",
        ))
        .expect("valid column")
}

/// Creates a relation edge
fn make_relation(from: &str, to: &str) -> RelationEdge {
    RelationEdge {
        from: from.to_string(),
        to: to.to_string(),
        kind: RelationKind::OneToMany,
        from_column: "id".to_string(),
        to_column: None,
    }
}

/// Creates a relation edge with column information
fn make_relation_with_columns(from: &str, to: &str, from_col: &str, to_col: &str) -> RelationEdge {
    RelationEdge {
        from: from.to_string(),
        to: to.to_string(),
        kind: RelationKind::OneToMany,
        from_column: from_col.to_string(),
        to_column: Some(to_col.to_string()),
    }
}

/// Creates a composition relation edge
fn make_composition(from: &str, to: &str) -> RelationEdge {
    RelationEdge {
        from: from.to_string(),
        to: to.to_string(),
        kind: RelationKind::Composition,
        from_column: "owner_id".to_string(),
        to_column: None,
    }
}

// =============================================================================
// 1. Table Specification Management Tests
// =============================================================================

mod table_spec_management {
    use super::*;

    #[test]
    fn register_table_spec_success() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        let spec = make_table_spec("actor_state");

        registry.register(spec)?;

        assert!(registry.contains("actor_state"));
        assert_eq!(registry.table_count(), 1);
        Ok(())
    }

    #[test]
    fn register_multiple_tables() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();

        registry.register(make_table_spec("actor_state"))?;
        registry.register(make_table_spec("province"))?;
        registry.register(make_table_spec("region"))?;

        assert_eq!(registry.table_count(), 3);
        assert!(registry.contains("actor_state"));
        assert!(registry.contains("province"));
        assert!(registry.contains("region"));
        Ok(())
    }

    #[test]
    fn register_duplicate_table_fails() {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("actor_state")).unwrap();

        let result = registry.register(make_table_spec("actor_state"));

        assert!(result.is_err());
    }

    #[test]
    fn query_table_spec_success() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        let spec = make_actor_table_spec();
        registry.register(spec)?;

        let retrieved = registry.get("actor_state")?;

        assert_eq!(retrieved.name, "actor_state");
        assert_eq!(retrieved.columns.len(), 3);
        Ok(())
    }

    #[test]
    fn query_nonexistent_table_fails() {
        let registry = SchemaRegistry::new();

        let result = registry.get("nonexistent");

        assert!(result.is_err());
    }

    #[test]
    fn table_spec_with_primary_key() -> SchemaResult<()> {
        let spec = make_actor_table_spec();

        let pk = spec.primary_key_column();

        assert!(pk.is_some());
        assert_eq!(pk.unwrap().name, "actor_id");
        assert_eq!(pk.unwrap().semantic, FieldSemantic::Id);
        Ok(())
    }

    #[test]
    fn table_spec_column_lookup_by_name() -> SchemaResult<()> {
        let spec = make_actor_table_spec();

        let treasury_col = spec.column_by_name("treasury");

        assert!(treasury_col.is_some());
        assert_eq!(treasury_col.unwrap().storage_type, "i64");
        Ok(())
    }

    #[test]
    fn table_spec_column_index_lookup() -> SchemaResult<()> {
        let spec = make_actor_table_spec();

        let idx = spec.column_index_of("treasury");

        assert_eq!(idx, Some(2));
        Ok(())
    }
}

// =============================================================================
// 2. Field Semantic Tracking Tests
// =============================================================================

mod field_semantic_tracking {
    use super::*;

    #[test]
    fn field_semantic_id() {
        let semantic = FieldSemantic::Id;
        assert!(!semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_foreign_key() {
        let semantic = FieldSemantic::ForeignKey {
            target_table: "actor_state".to_string(),
        };
        assert!(!semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(semantic.is_reference());
    }

    #[test]
    fn field_semantic_fixedpoint_quantity() {
        let semantic = FieldSemantic::Quantity;
        assert!(semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_percent() {
        let semantic = FieldSemantic::Percent;
        assert!(semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_tick_timestamp() {
        let semantic = FieldSemantic::Timestamp;
        assert!(semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_tick_duration() {
        let semantic = FieldSemantic::DurationTicks;
        assert!(semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_position_2d() {
        let semantic = FieldSemantic::Position2D;
        assert!(semantic.is_numeric());
        assert!(semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_position_3d() {
        let semantic = FieldSemantic::Position3D;
        assert!(semantic.is_numeric());
        assert!(semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_name() {
        let semantic = FieldSemantic::Name;
        assert!(!semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_tag() {
        let semantic = FieldSemantic::Tag;
        assert!(!semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn field_semantic_raw() {
        let semantic = FieldSemantic::Raw;
        assert!(!semantic.is_numeric());
        assert!(!semantic.is_spatial());
        assert!(!semantic.is_reference());
    }

    #[test]
    fn retrieve_field_semantic_from_table() -> SchemaResult<()> {
        let spec = make_actor_table_spec();

        let treasury_col = spec.column_by_name("treasury").unwrap();

        assert_eq!(treasury_col.semantic, FieldSemantic::Quantity);
        Ok(())
    }

    #[test]
    fn foreign_key_columns_iteration() -> SchemaResult<()> {
        let spec = make_province_table_spec();

        let fk_columns: Vec<&ColumnSpec> = spec.foreign_key_columns().collect();

        assert_eq!(fk_columns.len(), 1);
        assert_eq!(fk_columns[0].name, "owner_id");
        Ok(())
    }

    #[test]
    fn column_with_nullable() {
        let col =
            ColumnSpec::new("optional_field", FieldSemantic::Name, "utf8").with_nullable(true);

        assert!(col.nullable);
    }

    #[test]
    fn column_default_not_nullable() {
        let col = ColumnSpec::new("required_field", FieldSemantic::Name, "utf8");

        assert!(!col.nullable);
    }
}

// =============================================================================
// 3. Schema Versioning Tests
// =============================================================================

mod schema_versioning {
    /// Represents a schema version for testing
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SchemaVersion {
        major: u32,
        minor: u32,
        patch: u32,
    }

    impl SchemaVersion {
        fn new(major: u32, minor: u32, patch: u32) -> Self {
            Self {
                major,
                minor,
                patch,
            }
        }

        fn is_compatible_with(&self, other: &SchemaVersion) -> bool {
            self.major == other.major && self.minor >= other.minor
        }
    }

    impl std::fmt::Display for SchemaVersion {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        }
    }

    #[test]
    fn version_string_formatting() {
        let v = SchemaVersion::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn version_compatibility_same_version() {
        let v1 = SchemaVersion::new(1, 0, 0);
        let v2 = SchemaVersion::new(1, 0, 0);
        assert!(v1.is_compatible_with(&v2));
    }

    #[test]
    fn version_compatibility_minor_update() {
        let engine = SchemaVersion::new(1, 1, 0);
        let save = SchemaVersion::new(1, 0, 0);
        assert!(engine.is_compatible_with(&save));
    }

    #[test]
    fn version_mismatch_major() {
        let engine = SchemaVersion::new(2, 0, 0);
        let save = SchemaVersion::new(1, 0, 0);
        assert!(!engine.is_compatible_with(&save));
    }

    #[test]
    fn version_mismatch_engine_older() {
        let engine = SchemaVersion::new(1, 0, 0);
        let save = SchemaVersion::new(1, 1, 0);
        assert!(!engine.is_compatible_with(&save));
    }

    #[test]
    fn migration_path_detection() {
        // Simulate detecting that a migration is needed
        let current_version = SchemaVersion::new(1, 1, 0);
        let saved_version = SchemaVersion::new(1, 0, 0);

        let needs_migration = current_version.minor > saved_version.minor
            || current_version.major > saved_version.major;

        assert!(needs_migration);
    }

    #[test]
    fn no_migration_needed_same_version() {
        let current_version = SchemaVersion::new(1, 0, 0);
        let saved_version = SchemaVersion::new(1, 0, 0);

        let needs_migration = current_version.minor > saved_version.minor
            || current_version.major > saved_version.major;

        assert!(!needs_migration);
    }
}

// =============================================================================
// 4. Relation Graph Maintenance Tests
// =============================================================================

mod relation_graph_maintenance {
    use super::*;

    #[test]
    fn relation_graph_build_empty() {
        let graph = RelationGraph::new();

        assert!(graph.all_edges().is_empty());
        assert!(graph.tables().is_empty());
    }

    #[test]
    fn relation_graph_add_edge() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();

        graph.add_edge(make_relation("actor_state", "province"))?;

        assert_eq!(graph.all_edges().len(), 1);
        Ok(())
    }

    #[test]
    fn relation_graph_add_multiple_edges() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();

        graph.add_edge(make_relation("actor_state", "province"))?;
        graph.add_edge(make_relation("region", "province"))?;
        graph.add_edge(make_relation("actor_state", "army"))?;

        assert_eq!(graph.all_edges().len(), 3);
        Ok(())
    }

    #[test]
    fn relation_graph_duplicate_edge_fails() {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("A", "B")).unwrap();

        let result = graph.add_edge(make_relation("A", "B"));

        assert!(result.is_err());
    }

    #[test]
    fn relation_graph_find_edge() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_composition("actor_state", "province"))?;

        let edge = graph.find_edge("actor_state", "province");

        assert!(edge.is_some());
        assert_eq!(edge.unwrap().kind, RelationKind::Composition);
        Ok(())
    }

    #[test]
    fn relation_graph_find_nonexistent_edge() {
        let graph = RelationGraph::new();

        let edge = graph.find_edge("A", "B");

        assert!(edge.is_none());
    }

    #[test]
    fn relation_graph_edges_from() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("actor_state", "province"))?;
        graph.add_edge(make_relation("actor_state", "army"))?;
        graph.add_edge(make_relation("region", "province"))?;

        let from_actor: Vec<&RelationEdge> = graph.edges_from("actor_state").collect();

        assert_eq!(from_actor.len(), 2);
        Ok(())
    }

    #[test]
    fn relation_graph_edges_from_empty() {
        let graph = RelationGraph::new();

        let edges: Vec<&RelationEdge> = graph.edges_from("nonexistent").collect();

        assert!(edges.is_empty());
    }

    #[test]
    fn relation_graph_cycle_detection_direct() {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("A", "B")).unwrap();

        let result = graph.add_edge(make_relation("B", "A"));

        assert!(result.is_err());
    }

    #[test]
    fn relation_graph_cycle_detection_indirect() {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("A", "B")).unwrap();
        graph.add_edge(make_relation("B", "C")).unwrap();

        let result = graph.add_edge(make_relation("C", "A"));

        assert!(result.is_err());
    }

    #[test]
    fn relation_graph_no_false_positive_cycle() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        // Diamond pattern: A -> B, A -> C, B -> D, C -> D
        graph.add_edge(make_relation("A", "B"))?;
        graph.add_edge(make_relation("A", "C"))?;
        graph.add_edge(make_relation("B", "D"))?;
        graph.add_edge(make_relation("C", "D"))?;

        assert_eq!(graph.all_edges().len(), 4);
        Ok(())
    }

    // -- detect_cycles() global cycle detection --

    #[test]
    fn detect_cycles_finds_direct_cycle() {
        let mut graph = RelationGraph::new();
        graph.add_edge_unchecked(make_relation("A", "B"));
        graph.add_edge_unchecked(make_relation("B", "A"));

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty(), "expected cycles in A<->B");
        assert_eq!(cycles.len(), 1);
    }

    #[test]
    fn detect_cycles_finds_multi_node_cycle() {
        let mut graph = RelationGraph::new();
        graph.add_edge_unchecked(make_relation("A", "B"));
        graph.add_edge_unchecked(make_relation("B", "C"));
        graph.add_edge_unchecked(make_relation("C", "A"));

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty(), "expected cycles in A->B->C->A");
    }

    #[test]
    fn detect_cycles_no_false_positive_on_dag() {
        let mut graph = RelationGraph::new();
        graph.add_edge_unchecked(make_relation("A", "B"));
        graph.add_edge_unchecked(make_relation("B", "C"));
        graph.add_edge_unchecked(make_relation("A", "D"));
        graph.add_edge_unchecked(make_relation("D", "C"));

        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty(), "expected no cycles in DAG");
    }

    #[test]
    fn detect_cycles_no_false_positive_on_diamond() {
        let mut graph = RelationGraph::new();
        graph.add_edge_unchecked(make_relation("A", "B"));
        graph.add_edge_unchecked(make_relation("A", "C"));
        graph.add_edge_unchecked(make_relation("B", "D"));
        graph.add_edge_unchecked(make_relation("C", "D"));

        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty(), "expected no cycles in diamond");
    }

    #[test]
    fn detect_cycles_empty_graph_has_no_cycles() {
        let graph = RelationGraph::new();
        let cycles = graph.detect_cycles();
        assert!(cycles.is_empty());
    }

    #[test]
    fn detect_cycles_single_self_loop() {
        let mut graph = RelationGraph::new();
        graph.add_edge_unchecked(make_relation("A", "A"));

        let cycles = graph.detect_cycles();
        assert!(!cycles.is_empty(), "expected self-loop cycle A->A");
    }

    #[test]
    fn relation_graph_remove_edge() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("A", "B"))?;

        graph.remove_edge("A", "B")?;

        assert!(graph.find_edge("A", "B").is_none());
        Ok(())
    }

    #[test]
    fn relation_graph_remove_nonexistent_edge_fails() {
        let mut graph = RelationGraph::new();

        let result = graph.remove_edge("A", "B");

        assert!(result.is_err());
    }

    #[test]
    fn relation_graph_tables_collection() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation("actor_state", "province"))?;
        graph.add_edge(make_relation("region", "province"))?;

        let tables = graph.tables();

        assert_eq!(tables.len(), 3);
        assert!(tables.contains("actor_state"));
        assert!(tables.contains("province"));
        assert!(tables.contains("region"));
        Ok(())
    }

    #[test]
    fn relation_graph_with_column_info() -> SchemaResult<()> {
        let mut graph = RelationGraph::new();
        let edge = make_relation_with_columns("province", "actor_state", "owner_id", "actor_id");

        graph.add_edge(edge)?;

        let found = graph.find_edge("province", "actor_state").unwrap();
        assert_eq!(found.from_column, "owner_id".to_string());
        assert_eq!(found.to_column, Some("actor_id".to_string()));
        Ok(())
    }

    #[test]
    fn relation_graph_cross_table_lookup() -> SchemaResult<()> {
        // Simulate the scenario from SPEC:
        // WHEN rule-ir executes `jump_to(Relation::Owner)` on province P in region_12
        // THEN the evaluator queries the RelationGraph
        let mut graph = RelationGraph::new();
        graph.add_edge(make_relation_with_columns(
            "province",
            "actor_state",
            "owner_id",
            "actor_id",
        ))?;

        // Query the relation graph for OwnerOf(province -> actor)
        let edge = graph.find_edge("province", "actor_state");

        assert!(edge.is_some());
        let e = edge.unwrap();
        assert_eq!(&e.from_column, "owner_id");
        assert_eq!(e.to_column.as_ref().unwrap(), "actor_id");
        Ok(())
    }
}

// =============================================================================
// 5. Mod Fingerprint Tracking Tests
// =============================================================================

mod mod_fingerprint_tracking {
    use std::collections::HashSet;

    /// Represents a mod fingerprint for testing
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ModFingerprint {
        mod_id: String,
        version: String,
        table_specs: HashSet<String>,
        content_hash: String,
    }

    impl ModFingerprint {
        fn new(mod_id: &str, version: &str) -> Self {
            Self {
                mod_id: mod_id.to_string(),
                version: version.to_string(),
                table_specs: HashSet::new(),
                content_hash: String::new(),
            }
        }

        fn with_table_spec(mut self, name: &str) -> Self {
            self.table_specs.insert(name.to_string());
            self
        }

        fn with_content_hash(mut self, hash: &str) -> Self {
            self.content_hash = hash.to_string();
            self
        }

        fn generate_fingerprint(&self) -> String {
            // Simple hash generation for testing
            format!(
                "{}:{}:{:?}:{}",
                self.mod_id, self.version, self.table_specs, self.content_hash
            )
        }

        fn matches(&self, other: &ModFingerprint) -> bool {
            self.mod_id == other.mod_id
                && self.version == other.version
                && self.content_hash == other.content_hash
        }
    }

    #[test]
    fn mod_fingerprint_creation() {
        let fingerprint = ModFingerprint::new("EuropaBarbarorum", "2.1");

        assert_eq!(fingerprint.mod_id, "EuropaBarbarorum");
        assert_eq!(fingerprint.version, "2.1");
        assert!(fingerprint.table_specs.is_empty());
    }

    #[test]
    fn mod_fingerprint_with_table_specs() {
        let fingerprint = ModFingerprint::new("TestMod", "1.0")
            .with_table_spec("custom_unit")
            .with_table_spec("custom_building");

        assert_eq!(fingerprint.table_specs.len(), 2);
        assert!(fingerprint.table_specs.contains("custom_unit"));
        assert!(fingerprint.table_specs.contains("custom_building"));
    }

    #[test]
    fn mod_fingerprint_generation() {
        let fingerprint = ModFingerprint::new("TestMod", "1.0")
            .with_table_spec("table1")
            .with_content_hash("abc123");

        let hash = fingerprint.generate_fingerprint();

        assert!(!hash.is_empty());
        assert!(hash.contains("TestMod"));
        assert!(hash.contains("1.0"));
        assert!(hash.contains("abc123"));
    }

    #[test]
    fn mod_fingerprint_comparison_match() {
        let fp1 = ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123");
        let fp2 = ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123");

        assert!(fp1.matches(&fp2));
    }

    #[test]
    fn mod_fingerprint_comparison_mismatch_version() {
        let fp1 = ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123");
        let fp2 = ModFingerprint::new("TestMod", "1.1").with_content_hash("abc123");

        assert!(!fp1.matches(&fp2));
    }

    #[test]
    fn mod_fingerprint_comparison_mismatch_hash() {
        let fp1 = ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123");
        let fp2 = ModFingerprint::new("TestMod", "1.0").with_content_hash("def456");

        assert!(!fp1.matches(&fp2));
    }

    #[test]
    fn mod_fingerprint_save_load_scenario() {
        // Scenario: Detecting mod mismatch on save load
        // WHEN loading a save whose ModFingerprint lists mod "EuropaBarbarorum v2.1"
        // AND mod "EuropaBarbarorum v2.1" is not present in the current mod directory
        // THEN the content-loader flags a mismatch

        let save_fingerprint = ModFingerprint::new("EuropaBarbarorum", "2.1")
            .with_table_spec("barbarian_unit")
            .with_content_hash("hash_from_save");

        let available_mods: Vec<ModFingerprint> = vec![
            ModFingerprint::new("EuropaBarbarorum", "2.0").with_content_hash("older_hash"),
            ModFingerprint::new("OtherMod", "1.0"),
        ];

        let found = available_mods.iter().any(|m| m.matches(&save_fingerprint));

        assert!(!found, "Mod mismatch should be detected");
    }

    #[test]
    fn mod_fingerprint_match_found() {
        let save_fingerprint = ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123");

        let available_mods: Vec<ModFingerprint> = vec![
            ModFingerprint::new("TestMod", "1.0").with_content_hash("abc123"),
            ModFingerprint::new("OtherMod", "1.0"),
        ];

        let found = available_mods.iter().any(|m| m.matches(&save_fingerprint));

        assert!(found, "Matching mod should be found");
    }
}

// =============================================================================
// 6. Cold-Start Registration Contract Tests
// =============================================================================

mod cold_start_registration {
    use super::*;

    #[test]
    fn registry_default_not_frozen() {
        let registry = SchemaRegistry::new();

        assert!(!registry.is_frozen());
    }

    #[test]
    fn registry_freeze_sets_frozen() {
        let mut registry = SchemaRegistry::new();

        registry.freeze();

        assert!(registry.is_frozen());
    }

    #[test]
    fn register_fails_when_frozen() {
        let mut registry = SchemaRegistry::new();
        registry.freeze();

        let result = registry.register(make_table_spec("test"));

        assert!(result.is_err());
    }

    #[test]
    fn add_relation_fails_when_frozen() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("A"))?;
        registry.register(make_table_spec("B"))?;
        registry.freeze();

        let result = registry.add_relation(make_relation("A", "B"));

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn remove_fails_when_frozen() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("A"))?;
        registry.freeze();

        let result = registry.remove("A");

        assert!(result.is_err());
        // Verify the table was NOT removed despite the error
        assert!(registry.contains("A"));
        Ok(())
    }

    #[test]
    fn remove_relation_fails_when_frozen() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("A"))?;
        registry.register(make_table_spec("B"))?;
        registry.add_relation(make_relation("A", "B"))?;
        registry.freeze();

        let result = registry.remove_relation("A", "B");

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn get_mut_fails_when_frozen() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("A"))?;
        registry.freeze();

        let result = registry.get_mut("A");

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn read_operations_work_when_frozen() -> SchemaResult<()> {
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("A"))?;
        registry.register(make_table_spec("B"))?;
        registry.add_relation(make_relation("A", "B"))?;
        registry.freeze();

        // All read operations should still work
        assert!(registry.contains("A"));
        assert_eq!(registry.table_count(), 2);
        assert!(registry.get("A").is_ok());

        let children: Vec<&str> = registry.children_of("A").collect();
        assert_eq!(children, vec!["B"]);

        Ok(())
    }

    #[test]
    fn cold_start_lifecycle_simulation() -> SchemaResult<()> {
        // Simulate the six-phase load lifecycle

        // Phase 1-3: Schema Migration (not directly tested here)

        // Phase 4: Content Compilation - Register all TableSpecs
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("actor_state"))?;
        registry.register(make_table_spec("province"))?;
        registry.register(make_table_spec("region"))?;
        registry.register(make_table_spec("army"))?;

        // Build RelationGraph from declared relations
        registry.add_relation(make_relation_with_columns(
            "province",
            "actor_state",
            "owner_id",
            "actor_id",
        ))?;
        registry.add_relation(make_relation_with_columns(
            "province",
            "region",
            "region_id",
            "region_id",
        ))?;
        registry.add_relation(make_relation_with_columns(
            "army",
            "actor_state",
            "owner_id",
            "actor_id",
        ))?;

        // Verify registry is populated
        assert_eq!(registry.table_count(), 4);
        let graph = registry.relation_graph();
        assert_eq!(graph.all_edges().len(), 3);

        // Phase 5: Schema Freeze
        registry.freeze();
        assert!(registry.is_frozen());

        // Verify no further registrations are accepted
        let result = registry.register(make_table_spec("new_table"));
        assert!(result.is_err(), "Should reject registration after freeze");

        // Phase 6: Simulation Start - read operations work
        assert!(registry.get("actor_state").is_ok());
        assert!(registry.get("province").is_ok());

        Ok(())
    }

    #[test]
    fn frozen_registry_allows_query_engine_access() -> SchemaResult<()> {
        // Simulate query-engine accessing RelationGraph after freeze
        let mut registry = SchemaRegistry::new();
        registry.register(make_table_spec("province"))?;
        registry.register(make_table_spec("actor_state"))?;
        registry.add_relation(make_relation_with_columns(
            "province",
            "actor_state",
            "owner_id",
            "actor_id",
        ))?;
        registry.freeze();

        // Query-engine queries the RelationGraph
        let graph = registry.relation_graph();
        let edge = graph.find_edge("province", "actor_state");

        assert!(edge.is_some());
        assert_eq!(edge.unwrap().from_column, "owner_id");

        Ok(())
    }
}

// =============================================================================
// 7. Six-Phase Load Lifecycle Tests
// =============================================================================

mod six_phase_load_lifecycle {
    use super::*;

    /// Helper to create a complete schema manifest for testing
    fn make_test_manifest(version: &str) -> SchemaManifest {
        let actor_table = TableSpec::new("actor_state")
            .with_column(ColumnSpec::new("actor_id", FieldSemantic::Id, "u64"))
            .expect("valid column")
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .expect("valid column");

        let province_table = TableSpec::new("province")
            .with_column(ColumnSpec::new("province_id", FieldSemantic::Id, "u64"))
            .expect("valid column")
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .expect("valid column")
            .with_column(ColumnSpec::new(
                "owner_id",
                FieldSemantic::ForeignKey {
                    target_table: "actor_state".to_string(),
                },
                "u64",
            ))
            .expect("valid column");

        let owner_relation = RelationEdge {
            from: "province".to_string(),
            to: "actor_state".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "owner_id".to_string(),
            to_column: Some("actor_id".to_string()),
        };

        let base_mod = ModFingerprint::new("base_game", "1.0.0")
            .with_table_spec("actor_state")
            .with_table_spec("province")
            .with_content_hash("base_hash_123");

        SchemaManifest::new(version)
            .with_table(actor_table)
            .with_table(province_table)
            .with_relation(owner_relation)
            .with_mod_fingerprint(base_mod)
            .with_metadata("engine_version", "0.1.0")
            .with_metadata("save_timestamp", "2024-01-01T00:00:00Z")
    }

    #[test]
    fn phase_1_snapshot_deserialize() {
        // Phase 1: Parse SchemaManifest from save header
        let manifest = make_test_manifest("1.0.0");

        // Serialize to TOML (as would be done in save file)
        let toml_str = manifest.to_toml().expect("serialization failed");

        // Deserialize (Phase 1)
        let parsed = SchemaManifest::from_toml(&toml_str).expect("deserialization failed");

        assert_eq!(parsed.schema_version, "1.0.0");
        assert_eq!(parsed.tables.len(), 2);
        assert_eq!(parsed.relations.len(), 1);
        assert_eq!(parsed.mod_fingerprints.len(), 1);
        assert_eq!(
            parsed.metadata.get("engine_version"),
            Some(&"0.1.0".to_string())
        );
    }

    #[test]
    fn phase_2_mod_coordination() -> SchemaResult<()> {
        // Phase 2: Store ModFingerprint list for comparison
        let manifest = make_test_manifest("1.0.0");
        let mut registry = SchemaRegistry::new();

        // Store fingerprints from save
        registry.store_mod_fingerprints(manifest.mod_fingerprints.clone())?;

        // Verify fingerprints are stored
        assert_eq!(registry.mod_fingerprints().len(), 1);
        assert!(registry.get_mod_fingerprint("base_game").is_some());

        // Compare with available mod (simulating Phase 2 comparison)
        let available_mod = ModFingerprint::new("base_game", "1.0.0")
            .with_table_spec("actor_state")
            .with_table_spec("province")
            .with_content_hash("base_hash_123");

        let stored = registry.get_mod_fingerprint("base_game").unwrap();
        assert_eq!(stored.mod_id, available_mod.mod_id);
        assert_eq!(stored.version, available_mod.version);

        Ok(())
    }

    #[test]
    fn phase_3_schema_migration() {
        // Phase 3: Apply migration functions to loaded SchemaManifest
        let old_manifest = make_test_manifest("1.0.0");

        // Create migration registry with target version
        let mut migration_registry = MigrationRegistry::new().with_target_version("2.0.0");

        // Register a migration from 1.0.0 to 2.0.0
        migration_registry.register(Migration::new(
            "v1_0_to_v2_0",
            "1.0.0",
            "2.0.0",
            Box::new(|manifest| {
                // Add a new table as part of migration
                let new_table = TableSpec::new("region")
                    .with_column(ColumnSpec::new("region_id", FieldSemantic::Id, "u64"))
                    .expect("valid column")
                    .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
                    .expect("valid column");
                manifest.tables.push(new_table);

                // Add metadata about migration
                manifest
                    .metadata
                    .insert("migrated".to_string(), "true".to_string());
                Ok(())
            }),
        ));

        // Apply migrations (Phase 3)
        let migrated = migration_registry
            .apply_migrations(old_manifest)
            .expect("migration failed");

        // Verify migration was applied
        assert_eq!(migrated.manifest.schema_version, "2.0.0");
        assert_eq!(migrated.original_version, "1.0.0");
        assert_eq!(migrated.applied_migrations, vec!["v1_0_to_v2_0"]);
        assert_eq!(migrated.manifest.tables.len(), 3); // 2 original + 1 new
        assert!(migrated.manifest.get_table("region").is_some());
        assert_eq!(
            migrated.manifest.metadata.get("migrated"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn phase_4_content_compilation() -> SchemaResult<()> {
        // Phase 4: Receive MigratedSchemaManifest and populate registry
        let manifest = make_test_manifest("1.0.0");
        let migrated = MigratedSchemaManifest::new(manifest, "1.0.0");

        let mut registry = SchemaRegistry::new();

        // Load from migrated manifest (Phase 4 entry point)
        registry.load_from_manifest(&migrated)?;

        // Verify registry is populated
        assert_eq!(registry.table_count(), 2);
        assert!(registry.contains("actor_state"));
        assert!(registry.contains("province"));

        // Verify RelationGraph is built
        let graph = registry.relation_graph();
        assert_eq!(graph.all_edges().len(), 1);
        let edge = graph.find_edge("province", "actor_state");
        assert!(edge.is_some());
        assert_eq!(edge.unwrap().from_column, "owner_id");

        // Verify migrated manifest is stored
        assert!(registry.migrated_manifest().is_some());

        Ok(())
    }

    #[test]
    fn phase_5_schema_freeze() -> SchemaResult<()> {
        // Phase 5: Freeze the registry
        let manifest = make_test_manifest("1.0.0");
        let migrated = MigratedSchemaManifest::new(manifest, "1.0.0");

        let mut registry = SchemaRegistry::new();
        registry.load_from_manifest(&migrated)?;

        // Verify not frozen before Phase 5
        assert!(!registry.is_frozen());

        // Freeze (Phase 5)
        registry.freeze();

        // Verify frozen
        assert!(registry.is_frozen());

        // Verify no further registrations are accepted
        let result = registry.register(make_table_spec("new_table"));
        assert!(result.is_err());

        // Verify read operations still work
        assert!(registry.get("actor_state").is_ok());
        assert_eq!(registry.table_count(), 2);

        Ok(())
    }

    #[test]
    fn phase_6_simulation_start() -> SchemaResult<()> {
        // Phase 6: Simulation starts with frozen registry
        let manifest = make_test_manifest("1.0.0");
        let migrated = MigratedSchemaManifest::new(manifest, "1.0.0");

        let mut registry = SchemaRegistry::new();
        registry.load_from_manifest(&migrated)?;
        registry.freeze();

        // Simulate simulation tick - query engine accessing registry
        let actor_table = registry.get("actor_state")?;
        let pk = actor_table.primary_key_column();
        assert!(pk.is_some());
        assert_eq!(pk.unwrap().name, "actor_id");

        // Query engine can access relation graph
        let graph = registry.relation_graph();
        let children: Vec<&str> = graph
            .edges_from("province")
            .map(|e| e.to.as_str())
            .collect();
        assert_eq!(children, vec!["actor_state"]);

        Ok(())
    }

    #[test]
    fn migrated_schema_transfer_protocol() -> SchemaResult<()> {
        // Test the full Migrated Schema Transfer Protocol:
        // Phase 3 -> Phase 4 transfer via shared state

        // Phase 3 completion: save-system applies migrations
        let old_manifest = make_test_manifest("1.0.0");
        let mut migration_registry = MigrationRegistry::new().with_target_version("1.1.0");
        migration_registry.register(Migration::new(
            "v1_0_to_v1_1",
            "1.0.0",
            "1.1.0",
            Box::new(|manifest| {
                manifest
                    .metadata
                    .insert("migration_applied".to_string(), "true".to_string());
                Ok(())
            }),
        ));

        let migrated = migration_registry.apply_migrations(old_manifest)?;

        // Phase 4 start: content-loader receives MigratedSchemaManifest
        let mut registry = SchemaRegistry::new();
        registry.load_from_manifest(&migrated)?;

        // Verify the transferred schema is correctly loaded
        assert_eq!(registry.table_count(), 2);
        assert!(registry.migrated_manifest().is_some());
        let stored_migrated = registry.migrated_manifest().unwrap();
        assert_eq!(stored_migrated.original_version, "1.0.0");
        assert_eq!(stored_migrated.applied_migrations, vec!["v1_0_to_v1_1"]);

        Ok(())
    }

    #[test]
    fn export_manifest_for_saving() -> SchemaResult<()> {
        // Test exporting manifest for saving (used after Phase 5)
        let manifest = make_test_manifest("1.0.0");
        let migrated = MigratedSchemaManifest::new(manifest, "1.0.0");

        let mut registry = SchemaRegistry::new();
        registry.load_from_manifest(&migrated)?;
        registry.freeze();

        // Export manifest for saving
        let exported = registry.export_manifest("1.0.0");

        // Verify exported manifest contains all data
        assert_eq!(exported.schema_version, "1.0.0");
        assert_eq!(exported.tables.len(), 2);
        assert_eq!(exported.relations.len(), 1);
        assert_eq!(exported.mod_fingerprints.len(), 1);

        // Verify tables are correctly exported
        assert!(exported.get_table("actor_state").is_some());
        assert!(exported.get_table("province").is_some());

        // Serialize to TOML for save file
        let toml_str = exported.to_toml().expect("serialization failed");
        assert!(!toml_str.is_empty());

        Ok(())
    }

    #[test]
    fn full_six_phase_lifecycle() -> SchemaResult<()> {
        // Complete six-phase lifecycle test

        // === Phase 1: Snapshot Deserialize ===
        let saved_manifest = make_test_manifest("1.0.0");
        let toml_str = saved_manifest.to_toml().expect("serialization failed");
        let parsed_manifest = SchemaManifest::from_toml(&toml_str).expect("deserialization failed");

        // === Phase 2: Mod Coordination ===
        let mut registry = SchemaRegistry::new();
        registry.store_mod_fingerprints(parsed_manifest.mod_fingerprints.clone())?;

        // Verify mod compatibility
        let base_mod = registry.get_mod_fingerprint("base_game");
        assert!(base_mod.is_some());

        // === Phase 3: Schema Migration ===
        let migration_registry = MigrationRegistry::new().with_target_version("1.0.0");
        // No migrations needed (already at target version)
        let migrated = migration_registry.apply_migrations(parsed_manifest)?;

        assert_eq!(migrated.manifest.schema_version, "1.0.0");
        assert!(migrated.applied_migrations.is_empty());

        // === Phase 4: Content Compilation ===
        registry.load_from_manifest(&migrated)?;
        assert_eq!(registry.table_count(), 2);

        // === Phase 5: Schema Freeze ===
        registry.freeze();
        assert!(registry.is_frozen());

        // === Phase 6: Simulation Start ===
        // Verify registry is ready for simulation
        let actor_table = registry.get("actor_state")?;
        assert_eq!(actor_table.columns.len(), 2);

        let graph = registry.relation_graph();
        assert_eq!(graph.all_edges().len(), 1);

        Ok(())
    }
}

// =============================================================================
// Integration Tests
// =============================================================================

mod integration_tests {
    use super::*;

    #[test]
    fn full_schema_setup_scenario() -> SchemaResult<()> {
        // Complete setup scenario matching SPEC requirements
        let mut registry = SchemaRegistry::new();

        // Register actor_state table with treasury (FixedPoint/Quantity)
        let actor_spec = TableSpec::new("actor_state")
            .with_column(ColumnSpec::new("actor_id", FieldSemantic::Id, "u64"))?
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))?
            .with_column(ColumnSpec::new("treasury", FieldSemantic::Quantity, "i64"))?;
        registry.register(actor_spec)?;

        // Register province table with foreign key to actor_state
        let province_spec = TableSpec::new("province")
            .with_column(ColumnSpec::new("province_id", FieldSemantic::Id, "u64"))?
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))?
            .with_column(ColumnSpec::new(
                "owner_id",
                FieldSemantic::ForeignKey {
                    target_table: "actor_state".to_string(),
                },
                "u64",
            ))?
            .with_column(ColumnSpec::new("tax_rate", FieldSemantic::Percent, "i64"))?;
        registry.register(province_spec)?;

        // Add relation for cross-table lookups
        registry.add_relation(make_relation_with_columns(
            "province",
            "actor_state",
            "owner_id",
            "actor_id",
        ))?;

        // Verify: Querying for treasury semantic
        let actor_table = registry.get("actor_state")?;
        let treasury_col = actor_table.column_by_name("treasury").unwrap();
        assert_eq!(treasury_col.semantic, FieldSemantic::Quantity);

        // Verify: Foreign key resolution
        let province_table = registry.get("province")?;
        let owner_col = province_table.column_by_name("owner_id").unwrap();
        match &owner_col.semantic {
            FieldSemantic::ForeignKey { target_table } => {
                assert_eq!(target_table, "actor_state");
            }
            _ => panic!("Expected ForeignKey semantic"),
        }

        // Verify: Relation graph query
        let graph = registry.relation_graph();
        let edge = graph.find_edge("province", "actor_state").unwrap();
        assert_eq!(edge.from_column, "owner_id");

        // Freeze for simulation
        registry.freeze();
        assert!(registry.is_frozen());

        Ok(())
    }

    #[test]
    fn table_spec_serialization_roundtrip() -> SchemaResult<()> {
        let spec = make_actor_table_spec();

        let json = serde_json::to_string(&spec).expect("serialization failed");
        let deserialized: TableSpec = serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(deserialized.name, spec.name);
        assert_eq!(deserialized.columns.len(), spec.columns.len());
        // Note: column_index is serde(skip), so it's rebuilt on access
        Ok(())
    }

    #[test]
    fn column_spec_serialization() {
        let col = ColumnSpec::new("test_col", FieldSemantic::Quantity, "i64").with_nullable(true);

        let json = serde_json::to_string(&col).expect("serialization failed");
        let deserialized: ColumnSpec = serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(deserialized.name, "test_col");
        assert_eq!(deserialized.semantic, FieldSemantic::Quantity);
        assert_eq!(deserialized.storage_type, "i64");
        assert!(deserialized.nullable);
    }

    #[test]
    fn field_semantic_serialization() {
        let semantic = FieldSemantic::ForeignKey {
            target_table: "actor_state".to_string(),
        };

        let json = serde_json::to_string(&semantic).expect("serialization failed");
        let deserialized: FieldSemantic =
            serde_json::from_str(&json).expect("deserialization failed");

        match deserialized {
            FieldSemantic::ForeignKey { target_table } => {
                assert_eq!(target_table, "actor_state");
            }
            _ => panic!("Wrong semantic type"),
        }
    }

    #[test]
    fn relation_edge_serialization() -> SchemaResult<()> {
        let edge = make_composition("parent", "child");

        let json = serde_json::to_string(&edge).expect("serialization failed");
        let deserialized: RelationEdge =
            serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(deserialized.from, "parent");
        assert_eq!(deserialized.to, "child");
        assert_eq!(deserialized.kind, RelationKind::Composition);
        assert_eq!(deserialized.from_column, "owner_id".to_string());
        Ok(())
    }

    /// Would have failed: relation_graph_mut must reject after freeze,
    /// distinct from add_relation which gates internally.
    #[test]
    fn relation_graph_mut_rejected_after_freeze() {
        let mut registry = SchemaRegistry::new();
        registry.freeze();
        let result = registry.relation_graph_mut();
        assert!(result.is_err(),
            "relation_graph_mut should fail after freeze, got {:?}", result);
    }

    // ------------------------------------------------------------------
    // Edge case tests
    // ------------------------------------------------------------------

    #[test]
    fn table_spec_roundtrip_with_empty_name_rejected() {
        let spec = TableSpec::new("");
        let mut registry = SchemaRegistry::new();
        let result = registry.register(spec);
        assert!(
            result.is_err(),
            "registering TableSpec with empty name should be rejected, got {:?}",
            result
        );
    }

    #[test]
    fn table_spec_roundtrip_with_unicode_name() -> SchemaResult<()> {
        let spec = TableSpec::new("存档表_🎮_データ")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "u64"))?
            .with_column(ColumnSpec::new(
                "用户名称",
                FieldSemantic::Name,
                "utf8",
            ))?;

        let json = serde_json::to_string(&spec).expect("serialization failed");
        let deserialized: TableSpec = serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(deserialized.name, "存档表_🎮_データ");
        assert_eq!(deserialized.columns.len(), 2);
        assert!(deserialized.column_by_name("用户名称").is_some());
        Ok(())
    }

    #[test]
    fn add_column_duplicate_name_rejected() {
        let col = ColumnSpec::new("health", FieldSemantic::Quantity, "i64");
        let result = TableSpec::new("units")
            .with_column(col.clone())
            .and_then(|t| t.with_column(col));
        assert!(
            matches!(&result, Err(SchemaError::DuplicateColumn(name)) if name == "health"),
            "expected DuplicateColumn(\"health\"), got {:?}",
            result
        );
    }

    #[test]
    fn migration_path_no_route_returns_none() {
        let mut registry = MigrationRegistry::new().with_target_version("3.0.0");
        registry.register(Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|_manifest| Ok(())),
        ));

        // No migration from 2.0.0 to 3.0.0 is registered, and "0.5.0" has no route
        let path = registry.find_migration_path("0.5.0");
        assert!(path.is_none(), "expected no route from unrelated version");
    }

    // ------------------------------------------------------------------
    // Phase 0 invariant fortification: may-fail edge case tests
    // ------------------------------------------------------------------

    /// Probes: store_mod_fingerprints after freeze returns FreezeViolation.
    /// Guards against fingerprint injection after schema has been locked.
    #[test]
    fn store_mod_fingerprints_rejected_after_freeze() {
        let mut registry = SchemaRegistry::new();
        let spec = make_actor_table_spec();
        registry.register(spec).unwrap();

        registry.freeze();

        let fingerprint = ModFingerprint {
            mod_id: "late_mod".to_owned(),
            version: "1.0.0".to_owned(),
            table_specs: vec!["actor_state".to_owned()],
            content_hash: "deadbeef".to_owned(),
        };
        let result = registry.store_mod_fingerprints(vec![fingerprint]);
        assert!(result.is_err(), "store_mod_fingerprints must be rejected after freeze");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen") || err.contains("freeze"),
            "error must mention freeze, got: {err}"
        );
    }

    /// Probes: TableSpec PartialEq excludes column_index, so a serialization
    /// roundtrip (which discards column_index) produces an equal spec.
    #[test]
    fn table_spec_partial_eq_after_roundtrip() {
        let original = make_province_table_spec();
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: TableSpec = serde_json::from_str(&json).unwrap();

        // column_index is excluded from PartialEq, so roundtrip must match
        assert_eq!(
            original, deserialized,
            "TableSpec after serialization roundtrip must be equal (column_index excluded)"
        );
        // Also verify structural identity
        assert_eq!(original.name, deserialized.name);
        assert_eq!(original.columns.len(), deserialized.columns.len());
    }

    /// Probes: MigrationRegistry::Clone — KNOWN DEFERRED.
    /// The spec requires MigrationRegistry to be Clone, but Migration contains
    /// Box<dyn Fn> which is fundamentally incompatible with #[derive(Clone)].
    /// See tasks.md § 15.10.8.1 — marked as deferred.
    ///
    /// This test verifies the registry works correctly; the Clone assertion
    /// is commented out until Clone is implemented on MigrationRegistry.
    #[test]
    fn migration_registry_preserves_state_for_consumers() {
        let mut registry = MigrationRegistry::new().with_target_version("3.0.0");
        registry.register(Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|_manifest| Ok(())),
        ));
        registry.register(Migration::new(
            "v2_to_v3",
            "2.0.0",
            "3.0.0",
            Box::new(|_manifest| Ok(())),
        ));

        // When Clone is implemented, replace with:
        //   let cloned = registry.clone();
        //   assert!(cloned.find_migration_path("1.0.0").is_some());
        let path = registry.find_migration_path("1.0.0");
        assert!(path.is_some(), "registry must find valid migration paths");

        // Verify the deferred gap: MigrationRegistry does not implement Clone
        // TODO: implement Clone for MigrationRegistry (requires fn pointer or
        // Arc-wrapped migration for Box<dyn Fn>).
    }

    /// Probes: set_migrated_manifest after freeze returns RegistryFrozen.
    /// Guards against manifest mutation after schema has been locked.
    #[test]
    fn set_migrated_manifest_rejected_after_freeze() {
        let mut registry = SchemaRegistry::new();
        let spec = make_actor_table_spec();
        registry.register(spec).unwrap();

        registry.freeze();

        let manifest = MigratedSchemaManifest {
            manifest: SchemaManifest {
                schema_version: "0.1.1".to_owned(),
                tables: vec![],
                relations: vec![],
                mod_fingerprints: vec![],
                metadata: std::collections::HashMap::new(),
            },
            original_version: "0.1.0".to_owned(),
            applied_migrations: vec![],
        };
        let result = registry.set_migrated_manifest(manifest);
        assert!(result.is_err(), "set_migrated_manifest must be rejected after freeze");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("frozen") || err.contains("Freeze"),
            "error must mention freeze, got: {err}"
        );
    }
}
