//! scharnhorst_save: serialization and persistence layer.
//!
//! Provides snapshot persistence, IPC serialization, checkpoint & journal
//! truncation, schema migration, mod-aware load tolerance, and six-phase
//! load reconstruction.

pub mod checkpoint;
pub mod error;
pub mod load_reconstruction;
pub mod migration;
pub mod mod_tolerance;
pub mod snapshot_manager;
pub mod snapshot_persistence;

pub use checkpoint::{CheckpointManager, CheckpointingSaveJournal, RetentionPolicy};
pub use error::{SaveError, SaveResult};
pub use load_reconstruction::{LoadReconstruction, LoadReconstructionBuilder};
pub use migration::{MigrationPipeline, MigrationRegistry, MigrationStep};
pub use mod_tolerance::{ModLoadOutcome, ModToleranceChecker, ModTolerancePolicy};
pub use snapshot_manager::{SnapshotConfig, SnapshotInfo, SnapshotManager};
pub use snapshot_persistence::{PersistedSnapshot, SnapshotHeader, SnapshotPersistence};
