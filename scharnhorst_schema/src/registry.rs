use std::collections::HashMap;

use crate::error::{SchemaError, SchemaResult};
use crate::manifest::{MigratedSchemaManifest, ModFingerprint, SchemaManifest};
use crate::relation::{RelationEdge, RelationGraph};
use crate::table_spec::TableSpec;

/// Owns all table schemas and the relation graph between them.
///
/// # Six-Phase Load Lifecycle Integration
///
/// The `SchemaRegistry` participates in the six-phase load lifecycle as defined
/// in the save-system specification :
///
/// | Phase | Action | Schema-Registry Role |
/// |-------|--------|---------------------|
/// | **Phase 1** | Snapshot Deserialize | Parse `SchemaManifest` from save header via `SchemaManifest::from_toml` |
/// | **Phase 2** | Mod Coordination | Store `ModFingerprint` list via `store_mod_fingerprints` |
/// | **Phase 3** | Schema Migration | Apply migrations via `MigrationRegistry::apply_migrations` |
/// | **Phase 4** | Content Compilation | Receive `MigratedSchemaManifest` via `load_from_manifest`; register all `TableSpec`s; build `RelationGraph` |
/// | **Phase 5** | Schema Freeze | Call `freeze` to set `is_frozen = true` |
/// | **Phase 6** | Simulation Start | No role (registry is frozen) |
///
/// # Migrated Schema Transfer Protocol
///
/// ```text
/// Phase 3 completion:
/// save-system applies migrations -> MigratedSchemaManifest (in memory)
/// 鈫?(passed via shared SchemaRegistry state)
/// Phase 4 start:
/// content-loader receives MigratedSchemaManifest reference via load_from_manifest
/// content-loader compiles base + mods -> Arrow tables
/// schema-registry registers TableSpecs, builds RelationGraph
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaRegistry {
    tables: HashMap<String, TableSpec>,
    relations: RelationGraph,
    frozen: bool,
    /// Mod fingerprints stored during Phase 2 for compatibility checking.
    mod_fingerprints: Vec<ModFingerprint>,
    /// The migrated schema manifest received during Phase 4 (if any).
    migrated_manifest: Option<MigratedSchemaManifest>,
}

impl SchemaRegistry {
    /// Creates a new empty schema registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the registry is frozen and cannot be modified.
    ///
    /// Used in Phase 5 to verify the registry is frozen before simulation starts.
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// Freezes the registry, preventing further modifications.
    ///
    /// # Phase 5 Usage
    ///
    /// Called at the end of Phase 4 (Content Compilation) to freeze the registry
    /// before the simulation starts. Once frozen, no new `TableSpec` registrations
    /// are accepted until the next cold start.
    ///
    /// ```rust,ignore
    /// // In Phase 5:
    /// registry.freeze;
    /// assert!(registry.is_frozen);
    /// ```
    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    // =========================================================================
    // Phase 2: Mod Coordination
    // =========================================================================

    /// Stores the mod fingerprint list for compatibility checking.
    ///
    /// # Phase 2 Usage
    ///
    /// Called during Phase 2 (Mod Coordination) to store the mod fingerprints
    /// from the save file. These are used to compare against available mods on disk.
    ///
    /// ```rust,ignore
    /// // In save-system Phase 2:
    /// let fingerprints = manifest.mod_fingerprints;
    /// registry.store_mod_fingerprints(fingerprints)?;
    /// // Compare against available mods...
    /// ```
    pub fn store_mod_fingerprints(
        &mut self,
        fingerprints: Vec<ModFingerprint>,
    ) -> SchemaResult<()> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        self.mod_fingerprints = fingerprints;
        Ok(())
    }

    /// Returns the stored mod fingerprints (if any).
    pub fn mod_fingerprints(&self) -> &[ModFingerprint] {
        &self.mod_fingerprints
    }

    /// Returns the mod fingerprint for the given mod ID, if stored.
    pub fn get_mod_fingerprint(&self, mod_id: &str) -> Option<&ModFingerprint> {
        self.mod_fingerprints.iter().find(|m| m.mod_id == mod_id)
    }

    // =========================================================================
    // Phase 4: Content Compilation
    // =========================================================================

    /// Loads schema from a migrated manifest and registers all tables and relations.
    ///
    /// This is the primary entry point for Phase 4 (Content Compilation). The
    /// `content-loader` receives the `MigratedSchemaManifest` from Phase 3 and
    /// uses this method to populate the registry.
    ///
    /// # Phase 4 Usage
    ///
    /// ```rust,ignore
    /// // In content-loader Phase 4:
    /// let migrated_manifest = // received from Phase 3 via shared state
    /// registry.load_from_manifest(&migrated_manifest)?;
    /// // Registry now contains all tables and relations from the manifest
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::RegistryFrozen` if the registry is already frozen.
    pub fn load_from_manifest(&mut self, migrated: &MigratedSchemaManifest) -> SchemaResult<()> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }

        // Store the migrated manifest for later reference
        self.migrated_manifest = Some(migrated.clone());

        // Store mod fingerprints from the manifest
        self.mod_fingerprints = migrated.manifest.mod_fingerprints.clone();

        // Register all tables from the manifest
        for table in migrated.tables() {
            self.register(table.clone())?;
        }

        // Add all relations from the manifest
        for edge in migrated.relations() {
            self.add_relation(edge.clone())?;
        }

        // Global cycle detection: verify the complete relation graph
        let cycles = self.relations.detect_cycles();
        if !cycles.is_empty() {
            return Err(SchemaError::CircularRelation(format!(
                "cycle detected in relation graph: {:?}",
                cycles[0]
            )));
        }

        Ok(())
    }

    /// Returns the migrated manifest if one was loaded.
    pub fn migrated_manifest(&self) -> Option<&MigratedSchemaManifest> {
        self.migrated_manifest.as_ref()
    }

    /// Sets the migrated manifest on the registry (for audit/debug).
    ///
    /// Called during Phase 4 (Content Compilation) to store the
    /// `MigratedSchemaManifest` produced in Phase 3.
    pub fn set_migrated_manifest(&mut self, manifest: MigratedSchemaManifest) -> SchemaResult<()> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        self.migrated_manifest = Some(manifest);
        Ok(())
    }

    // =========================================================================
    // Phase 5+: Export for Saving
    // =========================================================================

    /// Exports the current registry state as a `SchemaManifest`.
    ///
    /// Used when saving the game to serialize the current schema state.
    ///
    /// # Usage
    ///
    /// ```rust,ignore
    /// // When saving:
    /// let manifest = registry.export_manifest("1.0.0");
    /// // Serialize manifest to save header...
    /// ```
    pub fn export_manifest(&self, schema_version: impl Into<String>) -> SchemaManifest {
        let mut manifest = SchemaManifest::new(schema_version);

        // Export all tables
        for table in self.tables.values() {
            manifest = manifest.with_table(table.clone());
        }

        // Export all relations
        for edge in self.relations.all_edges() {
            manifest = manifest.with_relation(edge.clone());
        }

        // Export mod fingerprints
        for fingerprint in &self.mod_fingerprints {
            manifest = manifest.with_mod_fingerprint(fingerprint.clone());
        }

        manifest
    }

    pub fn register(&mut self, spec: TableSpec) -> SchemaResult<()> {
        if spec.name.is_empty() {
            return Err(SchemaError::InvalidTableName(spec.name));
        }
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        if self.tables.contains_key(&spec.name) {
            return Err(SchemaError::TableAlreadyExists(spec.name));
        }
        self.tables.insert(spec.name.clone(), spec);
        Ok(())
    }

    pub fn get(&self, name: &str) -> SchemaResult<&TableSpec> {
        self.tables
            .get(name)
            .ok_or_else(|| SchemaError::TableNotFound(name.to_owned()))
    }

    pub fn get_mut(&mut self, name: &str) -> SchemaResult<&mut TableSpec> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        self.tables
            .get_mut(name)
            .ok_or_else(|| SchemaError::TableNotFound(name.to_owned()))
    }

    pub fn remove(&mut self, name: &str) -> SchemaResult<TableSpec> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        self.tables
            .remove(name)
            .ok_or_else(|| SchemaError::TableNotFound(name.to_owned()))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn add_relation(&mut self, edge: RelationEdge) -> SchemaResult<()> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        if !self.tables.contains_key(&edge.from) {
            return Err(SchemaError::TableNotFound(edge.from));
        }
        if !self.tables.contains_key(&edge.to) {
            return Err(SchemaError::TableNotFound(edge.to));
        }
        self.relations.add_edge(edge)
    }

    pub fn remove_relation(&mut self, from: &str, to: &str) -> SchemaResult<()> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        self.relations.remove_edge(from, to)
    }

    pub fn relation_graph(&self) -> &RelationGraph {
        &self.relations
    }

    pub fn relation_graph_mut(&mut self) -> SchemaResult<&mut RelationGraph> {
        if self.frozen {
            return Err(SchemaError::RegistryFrozen);
        }
        Ok(&mut self.relations)
    }

    /// Returns all tables that have a direct outgoing relation from `name`.
    pub fn children_of(&self, name: &str) -> impl Iterator<Item = &str> {
        self.relations.edges_from(name).map(|e| e.to.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field_semantic::FieldSemantic;
    use crate::table_spec::ColumnSpec;

    fn make_spec(name: &str) -> TableSpec {
        let col = ColumnSpec::new("id", FieldSemantic::Id, "u64");
        TableSpec::new(name).with_column(col).unwrap()
    }

    fn make_relation(from: &str, to: &str) -> RelationEdge {
        RelationEdge {
            from: from.to_string(),
            to: to.to_string(),
            kind: crate::relation::RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        }
    }

    // ---- register ----

    #[test]
    fn register_ok() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("Unit"))?;
        assert!(reg.contains("Unit"));
        assert_eq!(reg.table_count(), 1);
        Ok(())
    }

    #[test]
    fn register_duplicate_is_err() {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("Unit")).unwrap();
        let result = reg.register(make_spec("Unit"));
        assert_eq!(
            result,
            Err(SchemaError::TableAlreadyExists("Unit".to_string()))
        );
    }

    #[test]
    fn register_when_frozen_is_err() {
        let mut reg = SchemaRegistry::new();
        reg.freeze();
        let result = reg.register(make_spec("Unit"));
        assert!(result.is_err());
    }

    // ---- get / get_mut ----

    #[test]
    fn get_ok() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("Unit"))?;
        let spec = reg.get("Unit")?;
        assert_eq!(spec.name, "Unit");
        Ok(())
    }

    #[test]
    fn get_not_found() {
        let reg = SchemaRegistry::new();
        let result = reg.get("Missing");
        assert_eq!(
            result,
            Err(SchemaError::TableNotFound("Missing".to_string()))
        );
    }

    #[test]
    fn get_mut_not_found() {
        let mut reg = SchemaRegistry::new();
        let result = reg.get_mut("Missing");
        assert_eq!(
            result,
            Err(SchemaError::TableNotFound("Missing".to_string()))
        );
    }

    // ---- remove ----

    #[test]
    fn remove_ok() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("Unit"))?;
        let removed = reg.remove("Unit")?;
        assert_eq!(removed.name, "Unit");
        assert!(!reg.contains("Unit"));
        assert_eq!(reg.table_count(), 0);
        Ok(())
    }

    #[test]
    fn remove_not_found() {
        let mut reg = SchemaRegistry::new();
        let result = reg.remove("Missing");
        assert_eq!(
            result,
            Err(SchemaError::TableNotFound("Missing".to_string()))
        );
    }

    // ---- contains ----

    #[test]
    fn contains_true() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("Unit"))?;
        assert!(reg.contains("Unit"));
        Ok(())
    }

    #[test]
    fn contains_false() {
        let reg = SchemaRegistry::new();
        assert!(!reg.contains("Nothing"));
    }

    // ---- table_names / table_count ----

    #[test]
    fn table_names_and_count() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.register(make_spec("C"))?;
        assert_eq!(reg.table_count(), 3);
        let mut names: Vec<&str> = reg.table_names().collect();
        names.sort();
        assert_eq!(names, vec!["A", "B", "C"]);
        Ok(())
    }

    #[test]
    fn empty_registry() {
        let reg = SchemaRegistry::new();
        assert_eq!(reg.table_count(), 0);
        let names: Vec<&str> = reg.table_names().collect();
        assert!(names.is_empty());
    }

    // ---- add_relation ----

    #[test]
    fn add_relation_ok() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.add_relation(make_relation("A", "B"))?;
        let children: Vec<&str> = reg.children_of("A").collect();
        assert_eq!(children, vec!["B"]);
        Ok(())
    }

    #[test]
    fn add_relation_missing_from_table() {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("B")).unwrap();
        let result = reg.add_relation(make_relation("A", "B"));
        assert_eq!(result, Err(SchemaError::TableNotFound("A".to_string())));
    }

    #[test]
    fn add_relation_missing_to_table() {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A")).unwrap();
        let result = reg.add_relation(make_relation("A", "B"));
        assert_eq!(result, Err(SchemaError::TableNotFound("B".to_string())));
    }

    #[test]
    fn add_relation_when_frozen_is_err() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.freeze();
        let result = reg.add_relation(make_relation("A", "B"));
        assert!(result.is_err());
        Ok(())
    }

    // ---- children_of ----

    #[test]
    fn children_of_empty() {
        let reg = SchemaRegistry::new();
        let children: Vec<&str> = reg.children_of("X").collect();
        assert!(children.is_empty());
    }

    #[test]
    fn children_of_multiple() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.register(make_spec("C"))?;
        reg.add_relation(make_relation("A", "B"))?;
        reg.add_relation(make_relation("A", "C"))?;
        let mut children: Vec<&str> = reg.children_of("A").collect();
        children.sort();
        assert_eq!(children, vec!["B", "C"]);
        Ok(())
    }

    // ---- remove_relation ----

    #[test]
    fn remove_relation_ok() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.add_relation(make_relation("A", "B"))?;
        reg.remove_relation("A", "B")?;
        let children: Vec<&str> = reg.children_of("A").collect();
        assert!(children.is_empty());
        Ok(())
    }

    // ---- freeze ----

    #[test]
    fn default_is_not_frozen() {
        let reg = SchemaRegistry::new();
        assert!(!reg.is_frozen());
    }

    #[test]
    fn freeze_then_is_frozen() {
        let mut reg = SchemaRegistry::new();
        assert!(!reg.is_frozen());
        reg.freeze();
        assert!(reg.is_frozen());
    }

    #[test]
    fn frozen_prevents_register() {
        let mut reg = SchemaRegistry::new();
        reg.freeze();
        assert!(reg.register(make_spec("Unit")).is_err());
    }

    #[test]
    fn frozen_prevents_add_relation() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.freeze();
        assert!(reg.add_relation(make_relation("A", "B")).is_err());
        Ok(())
    }

    #[test]
    fn frozen_allows_read_operations() -> SchemaResult<()> {
        let mut reg = SchemaRegistry::new();
        reg.register(make_spec("A"))?;
        reg.register(make_spec("B"))?;
        reg.add_relation(make_relation("A", "B"))?;
        reg.freeze();
        // read operations should still work
        assert!(reg.contains("A"));
        assert_eq!(reg.table_count(), 2);
        assert!(reg.get("A").is_ok());
        let children: Vec<&str> = reg.children_of("A").collect();
        assert_eq!(children, vec!["B"]);
        Ok(())
    }
}
