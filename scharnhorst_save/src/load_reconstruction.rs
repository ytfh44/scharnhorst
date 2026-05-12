use scharnhorst_content::{
    migrated_from_manifest, LoadLifecycle, ModFingerprint,
};
use scharnhorst_core::Tick;
use scharnhorst_journal::CommitRecord;
use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};

use crate::error::{SaveError, SaveResult};
use crate::migration::MigrationPipeline;
use crate::mod_tolerance::ModToleranceChecker;
use crate::snapshot_persistence::SnapshotPersistence;

/// Six-phase load reconstruction state machine.
pub struct LoadReconstruction {
    lifecycle: LoadLifecycle,
    persistence: SnapshotPersistence,
    tolerance_checker: ModToleranceChecker,
    migration_pipeline: Option<MigrationPipeline>,
}

impl LoadReconstruction {
    pub fn new(
        persistence: SnapshotPersistence,
        tolerance_checker: ModToleranceChecker,
    ) -> Self {
        Self {
            lifecycle: LoadLifecycle::new(),
            persistence,
            tolerance_checker,
            migration_pipeline: None,
        }
    }

    pub fn with_migration_pipeline(mut self, pipeline: MigrationPipeline) -> Self {
        self.migration_pipeline = Some(pipeline);
        self
    }

    pub fn lifecycle(&self) -> &LoadLifecycle {
        &self.lifecycle
    }

 /// Phase 1: Deserialize the latest snapshot header.
    pub fn phase1_snapshot_deserialize(&mut self) -> SaveResult<SchemaManifest> {
        let (tick, _) = self
            .persistence
            .latest_snapshot()?
            .ok_or_else(|| SaveError::SnapshotNotFound("no snapshots available".to_owned()))?;
        let persisted = self.persistence.read_snapshot(tick)?;
        let manifest = persisted.header.schema_manifest;
        self.lifecycle.set_manifest(manifest.clone());
        Ok(manifest)
    }

 /// Phase 2: Compare stored mod fingerprints against available mods.
    pub fn phase2_mod_coordination(
        &mut self,
        stored: &scharnhorst_content::FingerprintRegistry,
        available: &[ModFingerprint],
    ) -> SaveResult<Vec<ModFingerprint>> {
        let _outcome = self.tolerance_checker.check(stored, available)?;
        let final_mods = self.tolerance_checker.final_mod_list(stored, available)?;
        self.lifecycle.set_final_mods(final_mods.clone());
        Ok(final_mods)
    }

 /// Phase 3: Apply schema migrations to bring manifest up to current version.
    pub fn phase3_schema_migration(
        &mut self,
        manifest: &SchemaManifest,
    ) -> SaveResult<MigratedSchemaManifest> {
        let migrated = match &self.migration_pipeline {
            Some(pipeline) => pipeline.apply(manifest)?,
            None => migrated_from_manifest(manifest, &manifest.schema_version),
        };
        self.lifecycle.set_migrated_manifest(migrated.clone());
        Ok(migrated)
    }

 /// Phase 4 entry point: the migrated manifest is handed off to the content loader.
    pub fn phase4_content_compilation_entry(
        &self,
    ) -> SaveResult<&MigratedSchemaManifest> {
        self.lifecycle
            .migrated_manifest()
            .ok_or_else(|| SaveError::Generic("migrated manifest not set".to_owned()))
    }

 /// Phase 5 entry point: signal that schema registry should freeze.
    pub fn phase5_schema_freeze(&mut self) -> SaveResult<()> {
        self.lifecycle.set_frozen(true);
        Ok(())
    }

 /// Phase 6 entry point: signal simulation start.
    pub fn phase6_simulation_start(&mut self) -> SaveResult<()> {
        self.lifecycle.finish().map_err(SaveError::Content)?;
        Ok(())
    }

 /// Replay journal diffs on top of a snapshot to reconstruct a later state.
 ///
 /// For each `CommitRecord`, the hash is computed identically to
 /// [`Journal::apply_diffs_and_hash`]: a `DefaultHasher` accumulates
 /// `tick`, `diffs.len`, and the JSON serialization of each diff.
 /// Record hashes are then accumulated via `wrapping_add`.
    pub fn replay_diffs(
        &self,
        base_tick: Tick,
        records: &[CommitRecord],
    ) -> SaveResult<u64> {
        use std::hash::{Hash, Hasher};

        let mut current_hash: u64 = 0;
        for record in records.iter().filter(|r| r.tick > base_tick) {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            record.tick.hash(&mut hasher);
            record.diffs.len().hash(&mut hasher);
            for diff in &record.diffs {
                let json = serde_json::to_string(diff)
                    .map_err(|e| SaveError::Generic(format!("diff serialize: {}", e)))?;
                json.hash(&mut hasher);
            }
            current_hash = current_hash.wrapping_add(hasher.finish());
        }
        Ok(current_hash)
    }

 /// Verify that a computed state hash matches the stored hash.
    pub fn verify_state_hash(computed: u64, expected: u64) -> SaveResult<()> {
        if computed != expected {
            return Err(SaveError::StateHashMismatch {
                expected,
                got: computed,
            });
        }
        Ok(())
    }
}

/// Builder for configuring and running the full six-phase load.
pub struct LoadReconstructionBuilder {
    persistence: Option<SnapshotPersistence>,
    tolerance_checker: Option<ModToleranceChecker>,
    migration_pipeline: Option<MigrationPipeline>,
}

impl LoadReconstructionBuilder {
    pub fn new() -> Self {
        Self {
            persistence: None,
            tolerance_checker: None,
            migration_pipeline: None,
        }
    }

    pub fn persistence(mut self, p: SnapshotPersistence) -> Self {
        self.persistence = Some(p);
        self
    }

    pub fn tolerance_checker(mut self, c: ModToleranceChecker) -> Self {
        self.tolerance_checker = Some(c);
        self
    }

    pub fn migration_pipeline(mut self, p: MigrationPipeline) -> Self {
        self.migration_pipeline = Some(p);
        self
    }

    pub fn build(self) -> SaveResult<LoadReconstruction> {
        let persistence = self
            .persistence
            .ok_or_else(|| SaveError::Generic("persistence required".to_owned()))?;
        let tolerance_checker = self
            .tolerance_checker
            .ok_or_else(|| SaveError::Generic("tolerance_checker required".to_owned()))?;
        let mut recon = LoadReconstruction::new(persistence, tolerance_checker);
        if let Some(pipeline) = self.migration_pipeline {
            recon = recon.with_migration_pipeline(pipeline);
        }
        Ok(recon)
    }
}

impl Default for LoadReconstructionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scharnhorst_content::{migrated_target_version, SchemaManifest};
use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("sch_load_test_{}", std::process::id()))
    }

    fn dummy_manifest() -> SchemaManifest {
        SchemaManifest::new("1.0.0")
    }

    #[test]
    fn builder_requires_persistence() {
        let result = LoadReconstructionBuilder::new()
            .tolerance_checker(ModToleranceChecker::default())
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn phase3_no_migration_needed() -> SaveResult<()> {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let persistence = SnapshotPersistence::new(&dir);
        let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
        let manifest = dummy_manifest();
        let migrated = recon.phase3_schema_migration(&manifest)?;
        assert_eq!(migrated_target_version(&migrated), "1.0.0");
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn replay_empty_diffs() -> SaveResult<()> {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let persistence = SnapshotPersistence::new(&dir);
        let recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
        let hash = recon.replay_diffs(Tick(0), &[])?;
        assert_eq!(hash, 0);
        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn verify_matching_hash() -> SaveResult<()> {
        LoadReconstruction::verify_state_hash(42, 42)?;
        Ok(())
    }

    #[test]
    fn verify_mismatched_hash() {
        let result = LoadReconstruction::verify_state_hash(42, 43);
        assert!(matches!(result, Err(SaveError::StateHashMismatch { .. })));
    }
}
