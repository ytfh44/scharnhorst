//! 10.3 Verify save/load cycle and schema migration.
//!
//! Uses the save-system snapshot persistence and migration pipeline.

use std::path::PathBuf;

use scharnhorst_content::{migrated_target_version, SchemaManifest};
use scharnhorst_core::Tick;
use scharnhorst_save::{
    CheckpointManager, MigrationPipeline, MigrationStep, MigrationRegistry,
    RetentionPolicy, SnapshotHeader, SnapshotPersistence, PersistedSnapshot,
};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

use std::sync::atomic::{AtomicUsize, Ordering};

static DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("sch_int_test_{}_{}", std::process::id(), n))
}

#[test]
fn save_load_snapshot_roundtrip() {
    let dir = temp_dir();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("0.1.0");
    let header = SnapshotHeader::new(manifest, Tick(7), 0x1234_5678_9abc_def0);
    let snapshot = PersistedSnapshot::new(header)
        .with_table_data("actors", vec![1, 2, 3])
        .with_table_data("spatial_nodes", vec![4, 5, 6]);

    let written = persistence.write_snapshot(&snapshot).expect("write snapshot");
    assert!(written.exists());

    let loaded = persistence.read_snapshot(Tick(7)).expect("read snapshot");
    assert_eq!(loaded.header.generation, Tick(7));
    assert_eq!(loaded.header.state_hash, 0x1234_5678_9abc_def0);
    assert_eq!(loaded.table_data.len(), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn migration_pipeline_upgrades_manifest() {
    let mut pipeline = MigrationPipeline::new("1.1.0");
    pipeline.register(MigrationStep::new(
        "1.0.0",
        "1.1.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let col = ColumnSpec::new("mood", FieldSemantic::Raw, "f64");
                let idx = table.columns.len();
                table.columns.push(col.clone());
                table.column_index.insert(col.name, idx);
            }
            Ok(())
        },
    ));

    let manifest = SchemaManifest::new("1.0.0")
        .with_table(TableSpec::new("actors").with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64")).unwrap_or_else(|_| TableSpec::new("actors")));

    let migrated = pipeline.apply(&manifest).expect("apply migration");
    assert_eq!(migrated_target_version(&migrated), "1.1.0");
    let actors = migrated.manifest().get_table("actors").expect("actors table");
    assert!(actors.column_by_name("mood").is_some());
}

#[test]
fn checkpoint_manager_triggers_auto_checkpoint() {
    let dir = temp_dir();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let journal = dir.join("journal.bin");

    let mut mgr = CheckpointManager::new(
        &dir,
        RetentionPolicy::default().with_auto_checkpoint_threshold(10),
        &journal,
    );

    for _ in 0..10 {
        mgr.record_journal_entry();
    }

    assert!(mgr.should_auto_checkpoint());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn migration_registry_routes_to_pipeline() {
    let mut registry = MigrationRegistry::new();
    registry.register(MigrationPipeline::new("2.0.0"));

    let manifest = SchemaManifest::new("2.0.0")
        .with_table(TableSpec::new("actors"));

    let migrated = registry.migrate(&manifest, "2.0.0").expect("migrate");
    assert_eq!(migrated_target_version(&migrated), "2.0.0");
}
