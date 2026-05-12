use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};
use scharnhorst_schema::{RelationEdge, SchemaRegistry, TableSpec};

use crate::error::ContentError;
use scharnhorst_schema::manifest::ModFingerprint;

// ---------------------------------------------------------------------------
// Content-specific helpers for SchemaManifest
// ---------------------------------------------------------------------------

/// Build a SchemaManifest from a populated SchemaRegistry.
pub fn manifest_from_registry(
    registry: &SchemaRegistry,
    fingerprints: Vec<ModFingerprint>,
    engine_version: impl Into<String>,
) -> SchemaManifest {
    let ev = engine_version.into();
    let tables: Vec<TableSpec> = registry
        .table_names()
        .filter_map(|name| registry.get(name).ok().cloned())
        .collect();
    let relations: Vec<RelationEdge> = registry
        .relation_graph()
        .all_edges()
        .to_vec();
    let mut manifest = SchemaManifest::new(&ev)
        .with_metadata("engine_version", &ev);
    for table in tables {
        manifest = manifest.with_table(table);
    }
    for edge in relations {
        manifest = manifest.with_relation(edge);
    }
    for fp in fingerprints {
        manifest = manifest.with_mod_fingerprint(fp);
    }
    manifest
}

/// Serialize a SchemaManifest to JSON bytes.
pub fn manifest_to_bytes(manifest: &SchemaManifest) -> Result<Vec<u8>, ContentError> {
    serde_json::to_vec(manifest).map_err(|e| {
        ContentError::ManifestSerialization(format!("json: {}", e))
    })
}

/// Deserialize a SchemaManifest from JSON bytes.
pub fn manifest_from_bytes(bytes: &[u8]) -> Result<SchemaManifest, ContentError> {
    serde_json::from_slice(bytes).map_err(|e| {
        ContentError::ManifestSerialization(format!("json: {}", e))
    })
}

/// Serialize a SchemaManifest to a TOML string.
pub fn manifest_to_toml(manifest: &SchemaManifest) -> Result<String, ContentError> {
    toml::to_string_pretty(manifest).map_err(|e| {
        ContentError::ManifestSerialization(format!("toml: {}", e))
    })
}

/// Deserialize a SchemaManifest from a TOML string.
pub fn manifest_from_toml(toml_str: &str) -> Result<SchemaManifest, ContentError> {
    toml::from_str(toml_str).map_err(|e| {
        ContentError::ManifestSerialization(format!("toml: {}", e))
    })
}

/// Retrieve the engine version from a SchemaManifest's metadata.
pub fn manifest_engine_version(manifest: &SchemaManifest) -> &str {
    manifest
        .metadata
        .get("engine_version")
        .map(|s| s.as_str())
        .unwrap_or(&manifest.schema_version)
}

// ---------------------------------------------------------------------------
// Content-specific helpers for MigratedSchemaManifest
// ---------------------------------------------------------------------------

/// Create a MigratedSchemaManifest from a SchemaManifest and target version.
///
/// Copies tables, relations, and other contents from the source manifest
/// into a new SchemaManifest with the given target version.
pub fn migrated_from_manifest(
    manifest: &SchemaManifest,
    target_version: impl Into<String>,
) -> MigratedSchemaManifest {
    let tv = target_version.into();
    let mut new_manifest = SchemaManifest::new(&tv);
    for table in &manifest.tables {
        new_manifest = new_manifest.with_table(table.clone());
    }
    for edge in &manifest.relations {
        new_manifest = new_manifest.with_relation(edge.clone());
    }
    for fp in &manifest.mod_fingerprints {
        new_manifest = new_manifest.with_mod_fingerprint(fp.clone());
    }
 // Copy metadata
    for (key, value) in &manifest.metadata {
        new_manifest = new_manifest.with_metadata(key, value);
    }
    MigratedSchemaManifest::new(new_manifest, manifest.schema_version.clone())
}

/// Look up a table by name within the underlying manifest of a MigratedSchemaManifest.
pub fn migrated_table_by_name<'a>(
    migrated: &'a MigratedSchemaManifest,
    name: &str,
) -> Option<&'a TableSpec> {
    migrated.manifest().get_table(name)
}

/// Return the target (migrated) version of a MigratedSchemaManifest.
pub fn migrated_target_version(migrated: &MigratedSchemaManifest) -> &str {
    &migrated.manifest().schema_version
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_schema::manifest::ModFingerprint;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, RelationEdge, RelationKind};

    #[test]
    fn from_registry_extracts_tables_and_relations() {
        let mut registry = SchemaRegistry::new();
        let spec_a = TableSpec::new("A")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "u64"))
            .unwrap();
        let spec_b = TableSpec::new("B")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "u64"))
            .unwrap();
        registry.register(spec_a).unwrap();
        registry.register(spec_b).unwrap();
        registry
            .add_relation(RelationEdge {
                from: "A".to_owned(),
                to: "B".to_owned(),
                kind: RelationKind::OneToMany,
                from_column: "id".to_string(),
                to_column: None,
            })
            .unwrap();

        let fp = ModFingerprint::new("base", "1.0");
        let manifest = manifest_from_registry(&registry, vec![fp], "0.1.0");
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(manifest.relations.len(), 1);
        assert_eq!(manifest.mod_fingerprints.len(), 1);
    }

    #[test]
    fn serialize_to_toml_roundtrip() {
        let spec = TableSpec::new("actors");
        let manifest = SchemaManifest::new("0.1.0")
            .with_metadata("engine_version", "0.1.0")
            .with_table(spec);
        let toml_str = manifest_to_toml(&manifest).unwrap();
        assert!(!toml_str.is_empty());
        let restored = manifest_from_toml(&toml_str).unwrap();
        assert_eq!(
            manifest.schema_version,
            restored.schema_version
        );
        assert_eq!(manifest.tables.len(), restored.tables.len());
    }

    #[test]
    fn serialize_to_toml_includes_fingerprints() {
        let fp = ModFingerprint::new("mod_x", "v2")
            .with_table_spec("t1")
            .with_content_hash("beef");
        let manifest = SchemaManifest::new("0.1.0")
            .with_metadata("engine_version", "0.1.0")
            .with_mod_fingerprint(fp);
        let toml_str = manifest_to_toml(&manifest).unwrap();
        assert!(toml_str.contains("mod_x"));
        assert!(toml_str.contains("beef"));
    }

    #[test]
    fn deserialize_from_toml_invalid() {
        let result = manifest_from_toml("not valid toml {{{");
        assert!(result.is_err());
    }

    #[test]
    fn manifest_constructors() {
        let manifest = SchemaManifest::new("1.0")
            .with_metadata("engine_version", "1.0")
            .with_table(TableSpec::new("t1"))
            .with_relation(RelationEdge {
                from: "t1".to_owned(),
                to: "t2".to_owned(),
                kind: RelationKind::Composition,
                from_column: "id".to_string(),
                to_column: None,
            })
            .with_mod_fingerprint(ModFingerprint::new("m1", "v1"));
        assert_eq!(manifest.tables.len(), 1);
        assert_eq!(manifest.relations.len(), 1);
        assert_eq!(manifest.mod_fingerprints.len(), 1);
    }

    #[test]
    fn to_bytes_roundtrip() {
        let manifest = SchemaManifest::new("0.1.0")
            .with_metadata("engine_version", "0.1.0")
            .with_table(TableSpec::new("actors"));
        let bytes = manifest_to_bytes(&manifest).unwrap();
        let restored = manifest_from_bytes(&bytes).unwrap();
        assert_eq!(
            manifest.schema_version,
            restored.schema_version
        );
        assert_eq!(manifest.tables.len(), restored.tables.len());
    }
}
