use crate::error::{ContentError, ContentResult};
use scharnhorst_schema::manifest::ModFingerprint;
use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};

/// The six phases of the cold-start load lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LoadPhase {
    /// Phase 1: Parse SchemaManifest from save header.
    SnapshotDeserialize,
    /// Phase 2: Compare ModFingerprint vs mods on disk.
    ModCoordination,
    /// Phase 3: Apply migration functions to SchemaManifest.
    SchemaMigration,
    /// Phase 4: Compile base + mods into Arrow tables.
    ContentCompilation,
    /// Phase 5: Freeze schema registry.
    SchemaFreeze,
    /// Phase 6: Begin simulation.
    SimulationStart,
}

impl LoadPhase {
    pub fn name(&self) -> &'static str {
        match self {
            LoadPhase::SnapshotDeserialize => "SnapshotDeserialize",
            LoadPhase::ModCoordination => "ModCoordination",
            LoadPhase::SchemaMigration => "SchemaMigration",
            LoadPhase::ContentCompilation => "ContentCompilation",
            LoadPhase::SchemaFreeze => "SchemaFreeze",
            LoadPhase::SimulationStart => "SimulationStart",
        }
    }

    pub fn next(&self) -> Option<LoadPhase> {
        match self {
            LoadPhase::SnapshotDeserialize => Some(LoadPhase::ModCoordination),
            LoadPhase::ModCoordination => Some(LoadPhase::SchemaMigration),
            LoadPhase::SchemaMigration => Some(LoadPhase::ContentCompilation),
            LoadPhase::ContentCompilation => Some(LoadPhase::SchemaFreeze),
            LoadPhase::SchemaFreeze => Some(LoadPhase::SimulationStart),
            LoadPhase::SimulationStart => None,
        }
    }
}

/// Tracks progress through the six-phase load lifecycle.
#[derive(Debug, Clone, Default)]
pub struct LoadLifecycle {
    current: Option<LoadPhase>,
    completed: Vec<LoadPhase>,
    manifest: Option<SchemaManifest>,
    migrated: Option<MigratedSchemaManifest>,
    final_mods: Vec<ModFingerprint>,
    frozen: bool,
}

impl LoadLifecycle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current active phase, if any.
    pub fn current_phase(&self) -> Option<LoadPhase> {
        self.current
    }

    /// Phases that have already completed.
    pub fn completed_phases(&self) -> &[LoadPhase] {
        &self.completed
    }

    /// Whether the given phase has been completed.
    pub fn is_completed(&self, phase: LoadPhase) -> bool {
        self.completed.contains(&phase)
    }

    /// Advance to the next phase.
    pub fn advance(&mut self) -> ContentResult<LoadPhase> {
        let next = match self.current {
            None => LoadPhase::SnapshotDeserialize,
            Some(phase) => phase
                .next()
                .ok_or_else(|| ContentError::InvalidPhaseTransition {
                    from: phase.name().to_owned(),
                    to: "(none)".to_owned(),
                })?,
        };

        if let Some(current) = self.current {
            if self.completed.contains(&current) {
                return Err(ContentError::PhaseAlreadyCompleted(
                    current.name().to_owned(),
                ));
            }
            self.completed.push(current);
        }

        self.current = Some(next);
        Ok(next)
    }

    /// Advance and mark the final phase as completed.
    pub fn finish(&mut self) -> ContentResult<()> {
        if let Some(current) = self.current {
            if !self.completed.contains(&current) {
                self.completed.push(current);
            }
        }
        self.current = None;
        Ok(())
    }

    /// Start a specific phase by name (for testing and recovery).
    pub fn start_phase(&mut self, phase: LoadPhase) -> ContentResult<()> {
        if let Some(current) = self.current {
            if current >= phase {
                return Err(ContentError::InvalidPhaseTransition {
                    from: current.name().to_owned(),
                    to: phase.name().to_owned(),
                });
            }
        }
        self.current = Some(phase);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Phase outputs
    // ------------------------------------------------------------------

    pub fn set_manifest(&mut self, manifest: SchemaManifest) {
        self.manifest = Some(manifest);
    }

    pub fn manifest(&self) -> Option<&SchemaManifest> {
        self.manifest.as_ref()
    }

    pub fn set_migrated_manifest(&mut self, migrated: MigratedSchemaManifest) {
        self.migrated = Some(migrated);
    }

    pub fn migrated_manifest(&self) -> Option<&MigratedSchemaManifest> {
        self.migrated.as_ref()
    }

    pub fn set_final_mods(&mut self, mods: Vec<ModFingerprint>) {
        self.final_mods = mods;
    }

    pub fn final_mods(&self) -> &[ModFingerprint] {
        &self.final_mods
    }

    pub fn set_frozen(&mut self, frozen: bool) {
        self.frozen = frozen;
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// True if all six phases are complete.
    pub fn is_finished(&self) -> bool {
        self.completed.len() == 6
    }
}

/// Coordinator that drives the lifecycle from outside.
pub struct LifecycleCoordinator;

impl LifecycleCoordinator {
    pub fn new() -> Self {
        Self
    }

    /// Run the full lifecycle from start to finish.
    pub fn run<F>(mut lifecycle: LoadLifecycle, mut phase_runner: F) -> ContentResult<LoadLifecycle>
    where
        F: FnMut(&mut LoadLifecycle, LoadPhase) -> ContentResult<()>,
    {
        loop {
            let phase = lifecycle.advance()?;
            phase_runner(&mut lifecycle, phase)?;
            if lifecycle.current_phase() == Some(LoadPhase::SimulationStart) {
                lifecycle.finish()?;
                break;
            }
        }
        Ok(lifecycle)
    }
}

impl Default for LifecycleCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_phases_have_unique_names() {
        let names: Vec<&str> = [
            LoadPhase::SnapshotDeserialize,
            LoadPhase::ModCoordination,
            LoadPhase::SchemaMigration,
            LoadPhase::ContentCompilation,
            LoadPhase::SchemaFreeze,
            LoadPhase::SimulationStart,
        ]
        .iter()
        .map(|p| p.name())
        .collect();
        let mut dedup: Vec<&str> = names.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(names.len(), dedup.len());
    }

    #[test]
    fn phase_transitions_are_linear() {
        let mut phases = Vec::new();
        let mut current = Some(LoadPhase::SnapshotDeserialize);
        while let Some(p) = current {
            phases.push(p);
            current = p.next();
        }
        assert_eq!(phases.len(), 6);
    }

    #[test]
    fn lifecycle_default_is_empty() {
        let lc = LoadLifecycle::new();
        assert!(lc.current_phase().is_none());
        assert!(lc.completed_phases().is_empty());
        assert!(lc.manifest().is_none());
        assert!(lc.migrated_manifest().is_none());
        assert!(lc.final_mods().is_empty());
        assert!(!lc.is_frozen());
    }

    #[test]
    fn set_final_mods_preserves_mods() {
        let mut lc = LoadLifecycle::new();
        let mods = vec![ModFingerprint::new("a", "1"), ModFingerprint::new("b", "2")];
        lc.set_final_mods(mods);
        assert_eq!(lc.final_mods().len(), 2);
    }

    #[test]
    fn lifecycle_coordinator_default() {
        let _coord = LifecycleCoordinator;
    }

    #[test]
    fn lifecycle_finish_with_no_current_is_ok() {
        let mut lc = LoadLifecycle::new();
        lc.finish().unwrap();
        assert!(lc.current_phase().is_none());
    }

    #[test]
    fn lifecycle_is_completed_per_phase() {
        let mut lc = LoadLifecycle::new();
        lc.advance().unwrap();
        lc.advance().unwrap();
        assert!(lc.is_completed(LoadPhase::SnapshotDeserialize));
        assert!(!lc.is_completed(LoadPhase::ModCoordination));
        assert!(lc.current_phase() == Some(LoadPhase::ModCoordination));
    }
}
