//! Schema migration system for the six-phase load lifecycle.
//!
//! This module provides the infrastructure for applying migrations to SchemaManifest
//! during Phase 3 of the load lifecycle, as defined in the save-system specification.

use crate::error::{SchemaError, SchemaResult};
use crate::manifest::{MigratedSchemaManifest, SchemaManifest};
use std::collections::HashMap;

/// A function that transforms a SchemaManifest from one version to another.
///
/// Migrations are applied in Phase 3 of the load lifecycle to bring saved schemas
/// up to the current engine version.
pub type MigrationFn = Box<dyn Fn(&mut SchemaManifest) -> SchemaResult<()> + Send + Sync>;

/// A named migration with source and target version information.
///
/// Note: `Migration` does not implement `Clone` because it contains a boxed
/// function pointer. If you need to share migrations, wrap them in `Arc`.
pub struct Migration {
 /// Human-readable name of this migration (e.g., "v1_0_to_v1_1").
    pub name: String,
 /// Source schema version this migration applies to.
    pub from_version: String,
 /// Target schema version after applying this migration.
    pub to_version: String,
 /// The migration function that performs the transformation.
    migration_fn: MigrationFn,
}

impl Migration {
 /// Creates a new migration with the given name and version range.
    pub fn new(
        name: impl Into<String>,
        from_version: impl Into<String>,
        to_version: impl Into<String>,
        migration_fn: MigrationFn,
    ) -> Self {
        Self {
            name: name.into(),
            from_version: from_version.into(),
            to_version: to_version.into(),
            migration_fn,
        }
    }

 /// Applies this migration to the given manifest.
 ///
 /// Returns an error if the manifest's current version doesn't match `from_version`.
    pub fn apply(&self, manifest: &mut SchemaManifest) -> SchemaResult<()> {
        if manifest.schema_version != self.from_version {
            return Err(SchemaError::MigrationVersionMismatch {
                expected: self.from_version.clone(),
                actual: manifest.schema_version.clone(),
            });
        }

        (self.migration_fn)(manifest)?;
        manifest.schema_version = self.to_version.clone();
        Ok(())
    }
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("name", &self.name)
            .field("from_version", &self.from_version)
            .field("to_version", &self.to_version)
            .finish_non_exhaustive()
    }
}

/// Registry of all available migrations, organized by version.
///
/// The migration registry is used in Phase 3 to find and apply the necessary
/// migrations to bring a saved schema up to the current engine version.
#[derive(Debug, Default)]
pub struct MigrationRegistry {
 /// Maps source version -> list of migrations starting from that version.
    migrations: HashMap<String, Vec<Migration>>,
 /// Target version that all migrations should eventually reach.
    target_version: String,
}

impl MigrationRegistry {
 /// Creates a new empty migration registry.
    pub fn new() -> Self {
        Self::default()
    }

 /// Sets the target version for migrations.
    pub fn with_target_version(mut self, version: impl Into<String>) -> Self {
        self.target_version = version.into();
        self
    }

 /// Registers a migration in this registry.
    pub fn register(&mut self, migration: Migration) {
        self.migrations
            .entry(migration.from_version.clone())
            .or_default()
            .push(migration);
    }

 /// Returns the target version for migrations.
    pub fn target_version(&self) -> &str {
        &self.target_version
    }

 /// Finds a migration path from the given version to the target version.
 ///
 /// Returns a list of migrations to apply in order, or None if no path exists.
    pub fn find_migration_path(&self, from_version: &str) -> Option<Vec<&Migration>> {
        if from_version == self.target_version {
            return Some(Vec::new());
        }

 // Simple BFS to find migration path
        let mut visited = std::collections::HashSet::new();
        let mut queue: Vec<(String, Vec<&Migration>)> = vec![(from_version.to_string(), Vec::new())];

        while let Some((current_version, path)) = queue.pop() {
            if current_version == self.target_version {
                return Some(path);
            }

            if !visited.insert(current_version.clone()) {
                continue;
            }

            if let Some(migrations) = self.migrations.get(&current_version) {
                for migration in migrations {
                    let mut new_path = path.clone();
                    new_path.push(migration);
                    queue.push((migration.to_version.clone(), new_path));
                }
            }
        }

        None
    }

 /// Applies all necessary migrations to bring the manifest to the target version.
 ///
 /// This is the primary method used in Phase 3 of the load lifecycle.
 /// Returns a `MigratedSchemaManifest` containing the migrated schema and
 /// information about what migrations were applied.
 ///
 /// # Phase 3 Usage
 ///
 /// ```rust,ignore
 /// // In save-system Phase 3:
 /// let migrated = migration_registry.apply_migrations(manifest)?;
 /// // Pass migrated to Phase 4 via shared state
 /// ```
    pub fn apply_migrations(&self, mut manifest: SchemaManifest) -> SchemaResult<MigratedSchemaManifest> {
        let original_version = manifest.schema_version.clone();

 // If already at target version, no migrations needed
        if original_version == self.target_version {
            return Ok(MigratedSchemaManifest::new(manifest, original_version));
        }

        let path = self
            .find_migration_path(&original_version)
            .ok_or_else(|| SchemaError::NoMigrationPath {
                from: original_version.clone(),
                to: self.target_version.clone(),
            })?;

        let mut applied = Vec::new();

        for migration in path {
            migration.apply(&mut manifest)?;
            applied.push(migration.name.clone());
        }

        Ok(MigratedSchemaManifest::new(manifest, original_version)
            .with_applied_migrations(applied))
    }
}

/// Extension trait for MigratedSchemaManifest to support batch migration recording.
pub trait MigratedSchemaManifestExt {
 /// Records multiple migrations as applied.
    fn with_applied_migrations(self, migrations: Vec<String>) -> Self;
}

impl MigratedSchemaManifestExt for MigratedSchemaManifest {
    fn with_applied_migrations(mut self, migrations: Vec<String>) -> Self {
        self.applied_migrations = migrations;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field_semantic::FieldSemantic;
    use crate::table_spec::{ColumnSpec, TableSpec};

    fn make_test_table(name: &str) -> TableSpec {
        let col = ColumnSpec::new("id", FieldSemantic::Id, "u64");
        TableSpec::new(name).with_column(col).unwrap()
    }

    fn make_test_manifest(version: &str) -> SchemaManifest {
        SchemaManifest::new(version).with_table(make_test_table("test_table"))
    }

 // ---- Migration ----

    #[test]
    fn migration_construction() {
        let migration = Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|manifest| {
                manifest.schema_version = "2.0.0".to_string();
                Ok(())
            }),
        );

        assert_eq!(migration.name, "v1_to_v2");
        assert_eq!(migration.from_version, "1.0.0");
        assert_eq!(migration.to_version, "2.0.0");
    }

    #[test]
    fn migration_apply_success() {
        let migration = Migration::new(
            "add_column",
            "1.0.0",
            "1.1.0",
            Box::new(|manifest| {
                if let Some(idx) = manifest.tables.iter().position(|t| t.name == "test_table") {
                    let table = manifest.tables.remove(idx);
                    let col = ColumnSpec::new("new_field", FieldSemantic::Quantity, "i64");
                    let updated_table = table.clone().with_column(col).unwrap_or(table);
                    manifest.tables.push(updated_table);
                }
                Ok(())
            }),
        );

        let mut manifest = make_test_manifest("1.0.0");
        assert_eq!(manifest.tables[0].columns.len(), 1);

        migration.apply(&mut manifest).unwrap();

        assert_eq!(manifest.schema_version, "1.1.0");
        assert_eq!(manifest.tables[0].columns.len(), 2);
    }

    #[test]
    fn migration_apply_version_mismatch() {
        let migration = Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|_manifest| Ok(())),
        );

        let mut manifest = make_test_manifest("0.9.0");
        let result = migration.apply(&mut manifest);

        assert!(matches!(
            result,
            Err(SchemaError::MigrationVersionMismatch { .. })
        ));
    }

 // ---- MigrationRegistry ----

    #[test]
    fn registry_construction() {
        let registry = MigrationRegistry::new().with_target_version("2.0.0");
        assert_eq!(registry.target_version(), "2.0.0");
    }

    #[test]
    fn registry_find_migration_path_direct() {
        let mut registry = MigrationRegistry::new().with_target_version("2.0.0");
        registry.register(Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|_manifest| Ok(())),
        ));

        let path = registry.find_migration_path("1.0.0").unwrap();
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].name, "v1_to_v2");
    }

    #[test]
    fn registry_find_migration_path_multi_step() {
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

        let path = registry.find_migration_path("1.0.0").unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].name, "v1_to_v2");
        assert_eq!(path[1].name, "v2_to_v3");
    }

    #[test]
    fn registry_find_migration_path_already_at_target() {
        let registry = MigrationRegistry::new().with_target_version("1.0.0");
        let path = registry.find_migration_path("1.0.0").unwrap();
        assert!(path.is_empty());
    }

    #[test]
    fn registry_find_migration_path_no_path() {
        let registry = MigrationRegistry::new().with_target_version("2.0.0");
        let path = registry.find_migration_path("1.0.0");
        assert!(path.is_none());
    }

    #[test]
    fn registry_apply_migrations_success() {
        let mut registry = MigrationRegistry::new().with_target_version("2.0.0");
        registry.register(Migration::new(
            "v1_to_v2",
            "1.0.0",
            "2.0.0",
            Box::new(|manifest| {
                manifest.metadata.insert("migrated".to_string(), "true".to_string());
                Ok(())
            }),
        ));

        let manifest = make_test_manifest("1.0.0");
        let migrated = registry.apply_migrations(manifest).unwrap();

        assert_eq!(migrated.manifest.schema_version, "2.0.0");
        assert_eq!(migrated.original_version, "1.0.0");
        assert_eq!(migrated.applied_migrations, vec!["v1_to_v2"]);
        assert_eq!(migrated.manifest.metadata.get("migrated"), Some(&"true".to_string()));
    }

    #[test]
    fn registry_apply_migrations_no_change_needed() {
        let registry = MigrationRegistry::new().with_target_version("1.0.0");
        let manifest = make_test_manifest("1.0.0");
        let migrated = registry.apply_migrations(manifest).unwrap();

        assert_eq!(migrated.manifest.schema_version, "1.0.0");
        assert_eq!(migrated.original_version, "1.0.0");
        assert!(migrated.applied_migrations.is_empty());
    }

    #[test]
    fn registry_apply_migrations_no_path() {
        let registry = MigrationRegistry::new().with_target_version("2.0.0");
        let manifest = make_test_manifest("1.0.0");
        let result = registry.apply_migrations(manifest);

        assert!(matches!(result, Err(SchemaError::NoMigrationPath { .. })));
    }
}
