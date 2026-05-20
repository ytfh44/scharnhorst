use std::collections::HashMap;
use std::sync::Arc;

use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};
use scharnhorst_schema::TableSpec;

use crate::error::{SaveError, SaveResult};

type MigrateTableFn = Arc<dyn Fn(&mut TableSpec) -> SaveResult<()> + Send + Sync>;

/// A single schema migration step from one version to another.
#[derive(Clone)]
pub struct MigrationStep {
    pub from_version: String,
    pub to_version: String,
    pub migrate_table: MigrateTableFn,
}

impl MigrationStep {
    pub fn new<F>(from: impl Into<String>, to: impl Into<String>, migrate_table: F) -> Self
    where
        F: Fn(&mut TableSpec) -> SaveResult<()> + Send + Sync + 'static,
    {
        Self {
            from_version: from.into(),
            to_version: to.into(),
            migrate_table: Arc::new(migrate_table),
        }
    }
}

/// A chain of migrations that can be applied to bring a [`SchemaManifest`] forward.
pub struct MigrationPipeline {
    steps: Vec<MigrationStep>,
    target_version: String,
}

impl std::fmt::Debug for MigrationPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationPipeline")
            .field("steps", &self.steps.len())
            .field("target_version", &self.target_version)
            .finish()
    }
}

impl Clone for MigrationPipeline {
    fn clone(&self) -> Self {
        Self {
            steps: self.steps.clone(),
            target_version: self.target_version.clone(),
        }
    }
}

impl MigrationPipeline {
    pub fn new(target_version: impl Into<String>) -> Self {
        Self {
            steps: Vec::new(),
            target_version: target_version.into(),
        }
    }

    pub fn target_version(&self) -> &str {
        &self.target_version
    }

    pub fn register(&mut self, step: MigrationStep) {
        self.steps.push(step);
    }

    /// Apply the full migration chain to a manifest.
    pub fn apply(&self, manifest: &SchemaManifest) -> SaveResult<MigratedSchemaManifest> {
        let mut current_version = manifest.schema_version.clone();
        let mut tables = manifest.tables.clone();
        let relations = manifest.relations.clone();

        let mut applied = 0usize;
        while current_version != self.target_version {
            let next_step = self
                .steps
                .iter()
                .find(|s| s.from_version == current_version)
                .ok_or_else(|| SaveError::NoMigrationPath(current_version.clone()))?;

            for table in &mut tables {
                (next_step.migrate_table)(table).map_err(|e| SaveError::MigrationFailed {
                    from: next_step.from_version.clone(),
                    to: next_step.to_version.clone(),
                    reason: e.to_string(),
                })?;
            }

            current_version = next_step.to_version.clone();
            applied += 1;
            if applied > self.steps.len() + 1 {
                return Err(SaveError::MigrationFailed {
                    from: manifest.schema_version.clone(),
                    to: self.target_version.clone(),
                    reason: "migration cycle detected".to_owned(),
                });
            }
        }

        // Build a new SchemaManifest with the target version and copy over migrated tables/relations
        let mut new_manifest = SchemaManifest::new(&self.target_version);
        for table in tables {
            new_manifest = new_manifest.with_table(table);
        }
        for edge in relations {
            new_manifest = new_manifest.with_relation(edge);
        }
        Ok(MigratedSchemaManifest::new(
            new_manifest,
            manifest.schema_version.clone(),
        ))
    }

    /// Return true if the given version is already at the target.
    pub fn is_up_to_date(&self, version: &str) -> bool {
        version == self.target_version
    }

    /// Return the number of registered steps.
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }
}

/// Registry of named migration pipelines keyed by target version.
#[derive(Debug, Clone, Default)]
pub struct MigrationRegistry {
    pipelines: HashMap<String, MigrationPipeline>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, pipeline: MigrationPipeline) {
        self.pipelines
            .insert(pipeline.target_version.clone(), pipeline);
    }

    pub fn get(&self, target_version: &str) -> Option<&MigrationPipeline> {
        self.pipelines.get(target_version)
    }

    pub fn migrate(
        &self,
        manifest: &SchemaManifest,
        target_version: &str,
    ) -> SaveResult<MigratedSchemaManifest> {
        let pipeline = self
            .get(target_version)
            .ok_or_else(|| SaveError::NoMigrationPath(target_version.to_owned()))?;
        pipeline.apply(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_content::migrated_target_version;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic};

    fn dummy_table(name: &str) -> TableSpec {
        TableSpec::new(name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap_or_else(|_| panic!("column"))
    }

    #[test]
    fn no_migration_needed() -> SaveResult<()> {
        let pipeline = MigrationPipeline::new("1.0.0");
        let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
        let migrated = pipeline.apply(&manifest)?;
        assert_eq!(migrated_target_version(&migrated), "1.0.0");
        assert_eq!(migrated.tables().len(), 1);
        Ok(())
    }

    #[test]
    fn single_step_migration() -> SaveResult<()> {
        let mut pipeline = MigrationPipeline::new("1.1.0");
        pipeline.register(MigrationStep::new(
            "1.0.0",
            "1.1.0",
            |table: &mut TableSpec| {
                if table.name == "actors" {
                    let idx = table.columns.len();
                    table.column_index.insert("mood".to_owned(), idx);
                    table
                        .columns
                        .push(ColumnSpec::new("mood", FieldSemantic::Raw, "f64"));
                }
                Ok(())
            },
        ));

        let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
        let migrated = pipeline.apply(&manifest)?;
        assert_eq!(migrated_target_version(&migrated), "1.1.0");
        let actors = migrated
            .manifest()
            .get_table("actors")
            .ok_or_else(|| SaveError::Generic("missing actors".to_owned()))?;
        assert!(actors.column_by_name("mood").is_some());
        Ok(())
    }

    #[test]
    fn missing_migration_path() {
        let pipeline = MigrationPipeline::new("2.0.0");
        let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
        let result = pipeline.apply(&manifest);
        assert!(matches!(result, Err(SaveError::NoMigrationPath(_))));
    }

    #[test]
    fn registry_lookup() -> SaveResult<()> {
        let mut registry = MigrationRegistry::new();
        registry.register(MigrationPipeline::new("1.1.0"));
        let manifest = SchemaManifest::new("1.1.0").with_table(dummy_table("actors"));
        let migrated = registry.migrate(&manifest, "1.1.0")?;
        assert_eq!(migrated_target_version(&migrated), "1.1.0");
        Ok(())
    }
}
