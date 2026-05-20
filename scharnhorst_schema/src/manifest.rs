//! SchemaManifest and ModFingerprint definitions for save/load lifecycle.
//!
//! This module provides the data structures needed for the six-phase load lifecycle
//! as defined in the save-system specification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::SchemaError;
use crate::relation::RelationEdge;
use crate::table_spec::TableSpec;

/// Fingerprint information for a loaded mod, stored in save files and used for
/// compatibility checking during the load lifecycle (Phase 2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModFingerprint {
    /// Mod ID as declared in the mod manifest.
    pub mod_id: String,
    /// Mod version as declared in the mod manifest.
    pub version: String,
    /// Set of TableSpec names registered by this mod.
    pub table_specs: Vec<String>,
    /// Content hash of the compiled Arrow data for this mod.
    pub content_hash: String,
}

impl ModFingerprint {
    /// Creates a new ModFingerprint with the given mod ID and version.
    pub fn new(mod_id: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            mod_id: mod_id.into(),
            version: version.into(),
            table_specs: Vec::new(),
            content_hash: String::new(),
        }
    }

    /// Adds a TableSpec name to this mod's fingerprint.
    pub fn with_table_spec(mut self, name: impl Into<String>) -> Self {
        self.table_specs.push(name.into());
        self
    }

    /// Sets the content hash for this mod.
    pub fn with_content_hash(mut self, hash: impl Into<String>) -> Self {
        self.content_hash = hash.into();
        self
    }
}

/// Schema manifest containing all table specifications, relations, and metadata.
///
/// This is the primary data structure serialized in save file headers and used
/// throughout the six-phase load lifecycle:
/// - Phase 1: Deserialized from save header
/// - Phase 3: Migrated to current engine version
/// - Phase 4: Used to build the schema registry and RelationGraph
/// - Phase 5+: Exported for saving (via `export_manifest`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaManifest {
    /// Schema format version for migration tracking.
    pub schema_version: String,
    /// All table specifications defined in this schema.
    pub tables: Vec<TableSpec>,
    /// All relation edges between tables.
    pub relations: Vec<RelationEdge>,
    /// Mod fingerprints for compatibility checking.
    pub mod_fingerprints: Vec<ModFingerprint>,
    /// Additional metadata (engine version, timestamp, etc.)
    pub metadata: HashMap<String, String>,
}

impl SchemaManifest {
    /// Creates a new empty schema manifest with the given schema version.
    pub fn new(schema_version: impl Into<String>) -> Self {
        Self {
            schema_version: schema_version.into(),
            tables: Vec::new(),
            relations: Vec::new(),
            mod_fingerprints: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Adds a TableSpec to this manifest.
    pub fn with_table(mut self, table: TableSpec) -> Self {
        self.tables.push(table);
        self
    }

    /// Adds a relation edge to this manifest.
    pub fn with_relation(mut self, edge: RelationEdge) -> Self {
        self.relations.push(edge);
        self
    }

    /// Adds a ModFingerprint to this manifest.
    pub fn with_mod_fingerprint(mut self, fingerprint: ModFingerprint) -> Self {
        self.mod_fingerprints.push(fingerprint);
        self
    }

    /// Adds metadata entry to this manifest.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Returns the TableSpec with the given name, if present.
    pub fn get_table(&self, name: &str) -> Option<&TableSpec> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Returns the ModFingerprint for the given mod ID, if present.
    pub fn get_mod_fingerprint(&self, mod_id: &str) -> Option<&ModFingerprint> {
        self.mod_fingerprints.iter().find(|m| m.mod_id == mod_id)
    }

    /// Serializes this manifest to a TOML string.
    ///
    /// Used when writing the manifest to save file headers.
    pub fn to_toml(&self) -> Result<String, SchemaError> {
        toml::to_string_pretty(self)
            .map_err(|e| SchemaError::ManifestSerialization(e.to_string()))
    }

    /// Deserializes a manifest from a TOML string.
    ///
    /// Used in Phase 1 of the load lifecycle when parsing save headers.
    pub fn from_toml(toml_str: &str) -> Result<Self, SchemaError> {
        toml::from_str(toml_str)
            .map_err(|e| SchemaError::ManifestDeserialization(e.to_string()))
    }

    /// Serializes this manifest to JSON.
    pub fn to_json(&self) -> Result<String, SchemaError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| SchemaError::ManifestSerialization(e.to_string()))
    }

    /// Deserializes a manifest from JSON.
    pub fn from_json(json_str: &str) -> Result<Self, SchemaError> {
        serde_json::from_str(json_str)
            .map_err(|e| SchemaError::ManifestDeserialization(e.to_string()))
    }
}

/// A migrated schema manifest, produced in Phase 3 and used in Phase 4.
///
/// This is a wrapper around SchemaManifest that indicates the schema has been
/// migrated to the current engine version and is ready for content compilation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigratedSchemaManifest {
    /// The underlying manifest (already migrated to current version).
    pub manifest: SchemaManifest,
    /// The original schema version before migration.
    pub original_version: String,
    /// List of migrations that were applied.
    pub applied_migrations: Vec<String>,
}

impl MigratedSchemaManifest {
    /// Creates a new migrated manifest from the given manifest.
    pub fn new(manifest: SchemaManifest, original_version: impl Into<String>) -> Self {
        Self {
            manifest,
            original_version: original_version.into(),
            applied_migrations: Vec::new(),
        }
    }

    /// Records that a migration was applied.
    pub fn record_migration(mut self, migration_name: impl Into<String>) -> Self {
        self.applied_migrations.push(migration_name.into());
        self
    }

    /// Returns a reference to the underlying manifest.
    pub fn manifest(&self) -> &SchemaManifest {
        &self.manifest
    }

    /// Returns the tables from the underlying manifest.
    pub fn tables(&self) -> &[TableSpec] {
        &self.manifest.tables
    }

    /// Returns the relations from the underlying manifest.
    pub fn relations(&self) -> &[RelationEdge] {
        &self.manifest.relations
    }

    /// Returns the mod fingerprints from the underlying manifest.
    pub fn mod_fingerprints(&self) -> &[ModFingerprint] {
        &self.manifest.mod_fingerprints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field_semantic::FieldSemantic;
    use crate::relation::{RelationEdge, RelationKind};
    use crate::table_spec::{ColumnSpec, TableSpec};

    fn make_test_table(name: &str) -> TableSpec {
        let col = ColumnSpec::new("id", FieldSemantic::Id, "u64");
        TableSpec::new(name).with_column(col).unwrap()
    }

    fn make_test_relation(from: &str, to: &str) -> RelationEdge {
        RelationEdge {
            from: from.to_string(),
            to: to.to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        }
    }

    // ---- ModFingerprint ----

    #[test]
    fn mod_fingerprint_construction() {
        let fp = ModFingerprint::new("test_mod", "1.0.0")
            .with_table_spec("actor_state")
            .with_table_spec("province_state")
            .with_content_hash("abc123");

        assert_eq!(fp.mod_id, "test_mod");
        assert_eq!(fp.version, "1.0.0");
        assert_eq!(fp.table_specs, vec!["actor_state", "province_state"]);
        assert_eq!(fp.content_hash, "abc123");
    }

    // ---- SchemaManifest ----

    #[test]
    fn schema_manifest_construction() {
        let manifest = SchemaManifest::new("1.0.0")
            .with_table(make_test_table("actor_state"))
            .with_table(make_test_table("province_state"))
            .with_relation(make_test_relation("actor_state", "province_state"))
            .with_metadata("engine_version", "0.1.0");

        assert_eq!(manifest.schema_version, "1.0.0");
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(manifest.relations.len(), 1);
        assert_eq!(
            manifest.metadata.get("engine_version"),
            Some(&"0.1.0".to_string())
        );
    }

    #[test]
    fn schema_manifest_get_table() {
        let manifest = SchemaManifest::new("1.0.0")
            .with_table(make_test_table("actor_state"))
            .with_table(make_test_table("province_state"));

        assert!(manifest.get_table("actor_state").is_some());
        assert!(manifest.get_table("nonexistent").is_none());
    }

    #[test]
    fn schema_manifest_get_mod_fingerprint() {
        let fp = ModFingerprint::new("test_mod", "1.0.0");
        let manifest = SchemaManifest::new("1.0.0").with_mod_fingerprint(fp);

        assert!(manifest.get_mod_fingerprint("test_mod").is_some());
        assert!(manifest.get_mod_fingerprint("nonexistent").is_none());
    }

    #[test]
    fn schema_manifest_json_roundtrip() {
        let manifest = SchemaManifest::new("1.0.0")
            .with_table(make_test_table("actor_state"))
            .with_relation(make_test_relation("actor_state", "province_state"))
            .with_mod_fingerprint(ModFingerprint::new("base", "1.0.0"));

        let json = manifest.to_json().unwrap();
        let restored = SchemaManifest::from_json(&json).unwrap();

        assert_eq!(restored.schema_version, manifest.schema_version);
        assert_eq!(restored.tables.len(), manifest.tables.len());
        assert_eq!(restored.relations.len(), manifest.relations.len());
        assert_eq!(
            restored.mod_fingerprints.len(),
            manifest.mod_fingerprints.len()
        );
    }

    // ---- MigratedSchemaManifest ----

    #[test]
    fn migrated_schema_manifest_construction() {
        let manifest = SchemaManifest::new("1.1.0").with_table(make_test_table("actor_state"));

        let migrated =
            MigratedSchemaManifest::new(manifest, "1.0.0").record_migration("v1_0_to_v1_1");

        assert_eq!(migrated.original_version, "1.0.0");
        assert_eq!(migrated.manifest.schema_version, "1.1.0");
        assert_eq!(migrated.applied_migrations, vec!["v1_0_to_v1_1"]);
    }

    #[test]
    fn migrated_schema_manifest_accessors() {
        let manifest = SchemaManifest::new("1.0.0")
            .with_table(make_test_table("actor_state"))
            .with_relation(make_test_relation("a", "b"))
            .with_mod_fingerprint(ModFingerprint::new("mod1", "1.0.0"));

        let migrated = MigratedSchemaManifest::new(manifest, "0.9.0");

        assert_eq!(migrated.tables().len(), 1);
        assert_eq!(migrated.relations().len(), 1);
        assert_eq!(migrated.mod_fingerprints().len(), 1);
    }
}
