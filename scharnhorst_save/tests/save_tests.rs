use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use scharnhorst_arrow_store::IpcBuffer;
use scharnhorst_content::{
    migrated_target_version, FingerprintRegistry, LoadPhase, ModFingerprint, SchemaManifest,
};
use scharnhorst_core::Tick;
use scharnhorst_core::{StateTier, TierRegistry};
use scharnhorst_journal::{CommitRecord, Diff, SaveJournal};
use scharnhorst_save::{
    CheckpointManager, CheckpointingSaveJournal, LoadReconstruction, LoadReconstructionBuilder,
    MigrationPipeline, MigrationRegistry, MigrationStep, ModLoadOutcome, ModToleranceChecker,
    ModTolerancePolicy, PersistedSnapshot, RetentionPolicy, SaveError, SaveResult, SnapshotHeader,
    SnapshotPersistence,
};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "sch_save_integration_{}_{}",
        std::process::id(),
        id
    ))
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn dummy_table(name: &str) -> TableSpec {
    TableSpec::new(name)
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
        .unwrap_or_else(|_| panic!("column"))
}

fn fp(mod_id: &str, version: &str, hash: &str) -> ModFingerprint {
    ModFingerprint::new(mod_id, version).with_content_hash(hash)
}

// ------------------------------------------------------------------
// Snapshot Persistence
// ------------------------------------------------------------------

#[test]
fn snapshot_list_and_latest() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0");

    for gen in [10u64, 5, 20] {
        let snapshot =
            PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(gen), gen))
                .with_table_data("actors", vec![1, 2, 3]);
        persistence.write_snapshot(&snapshot)?;
    }

    let list = persistence.list_snapshots()?;
    assert_eq!(list.len(), 3);
    assert_eq!(list[0].0, Tick(5));
    assert_eq!(list[2].0, Tick(20));

    let latest = persistence.latest_snapshot()?;
    assert_eq!(latest.map(|(t, _)| t), Some(Tick(20)));

    cleanup(&dir);
    Ok(())
}

// ------------------------------------------------------------------
// Authority tier filtering
// ------------------------------------------------------------------

#[test]
fn persisted_snapshot_filter_to_authority_removes_non_authority_tables() {
    use scharnhorst_core::StateTier;

    let mut tier_registry = TierRegistry::new();
    let _ = tier_registry.register(
        "actors",
        StateTier::Authority,
        "save_test",
        "authority table",
    );
    let _ = tier_registry.register(
        "ui_cache",
        StateTier::Derived,
        "save_test",
        "derived cache",
    );
    let _ = tier_registry.register(
        "input_buffer",
        StateTier::Ephemeral,
        "save_test",
        "ephemeral state",
    );
    let _ = tier_registry.register(
        "provinces",
        StateTier::Authority,
        "save_test",
        "authority table",
    );

    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"))
        .with_table(dummy_table("ui_cache"))
        .with_table(dummy_table("input_buffer"));

    let mut snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 100))
        .with_table_data("actors", vec![1u8])
        .with_table_data("provinces", vec![2u8])
        .with_table_data("ui_cache", vec![3u8])
        .with_table_data("input_buffer", vec![4u8]);

    assert_eq!(snapshot.table_data.len(), 4);

    snapshot.filter_to_authority(&tier_registry);

    assert_eq!(snapshot.table_data.len(), 2);
    assert!(snapshot.table_data.contains_key("actors"));
    assert!(snapshot.table_data.contains_key("provinces"));
    assert!(!snapshot.table_data.contains_key("ui_cache"));
    assert!(!snapshot.table_data.contains_key("input_buffer"));
}

#[test]
fn filter_to_authority_without_registry_removes_all_unregistered() {
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("ui_cache"));

    let mut snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 100))
        .with_table_data("actors", vec![1u8])
        .with_table_data("ui_cache", vec![2u8]);

    let empty_registry = TierRegistry::new();
    snapshot.filter_to_authority(&empty_registry);
    assert_eq!(snapshot.table_data.len(), 0,
        "unregistered tables are treated as non-Authority per spec");
}

#[test]
fn filter_to_authority_with_no_authority_tables_removes_all() {
    let mut tier_registry = TierRegistry::new();
    let _ = tier_registry.register(
        "data",
        StateTier::Derived,
        "test",
        "derived",
    );

    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("data"));
    let mut snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 100))
        .with_table_data("data", vec![1u8]);

    snapshot.filter_to_authority(&tier_registry);
    assert_eq!(snapshot.table_data.len(), 0);
}

#[test]
fn snapshot_delete_oldest() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0");

    for gen in [1u64, 2, 3, 4] {
        let snapshot =
            PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(gen), gen));
        persistence.write_snapshot(&snapshot)?;
    }

    persistence.delete_snapshot(Tick(1))?;
    let list = persistence.list_snapshots()?;
    assert_eq!(list.len(), 3);
    assert!(!list.iter().any(|(t, _)| *t == Tick(1)));

    cleanup(&dir);
    Ok(())
}

// ------------------------------------------------------------------
// IPC Serialization
// ------------------------------------------------------------------

#[test]
fn ipc_buffer_clear_and_concat() -> SaveResult<()> {
    let mut buf = IpcBuffer::new();
    buf.push(vec![1, 2, 3]);
    buf.push(vec![4, 5]);
    assert_eq!(buf.chunk_count(), 2);
    assert_eq!(buf.total_len(), 5);

    let bytes = buf.into_bytes();
    assert_eq!(bytes, vec![1, 2, 3, 4, 5]);
    Ok(())
}

// ------------------------------------------------------------------
// Checkpoint & Retention
// ------------------------------------------------------------------

#[test]
fn retention_enforces_max_snapshots() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;
    let journal = dir.join("journal.bin");

    let policy = RetentionPolicy::default().with_max_snapshots(2);
    let mgr = CheckpointManager::new(&dir, policy, &journal);
    let persistence = mgr.persistence();
    let manifest = SchemaManifest::new("1.0.0");

    for gen in [1u64, 2, 3, 4] {
        let snapshot =
            PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(gen), gen));
        persistence.write_snapshot(&snapshot)?;
    }

    mgr.enforce_retention()?;
    let list = persistence.list_snapshots()?;
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].0, Tick(3));
    assert_eq!(list[1].0, Tick(4));

    cleanup(&dir);
    Ok(())
}

#[test]
fn checkpointing_journal_records_and_truncates() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;
    let journal_path = dir.join("journal.bin");

    let policy = RetentionPolicy::default();
    let mgr = CheckpointManager::new(&dir, policy, &journal_path);
    let mut journal = CheckpointingSaveJournal::new().with_manager(mgr);

    for tick in 1..=5 {
        let record = CommitRecord::new(Tick(tick), Vec::new(), tick);
        journal.append(&record).map_err(SaveError::Journal)?;
    }

    assert_eq!(journal.len(), 5);
    journal
        .truncate_before(Tick(3))
        .map_err(SaveError::Journal)?;
    assert_eq!(journal.len(), 3);

    let mgr = journal.manager_mut().unwrap();
    mgr.truncate_journal()?;
    assert!(!journal_path.exists());

    cleanup(&dir);
    Ok(())
}

#[test]
fn auto_checkpoint_threshold_respected() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);
    let journal = dir.join("journal.bin");

    let policy = RetentionPolicy::default().with_auto_checkpoint_threshold(3);
    let mut mgr = CheckpointManager::new(&dir, policy, &journal);

    mgr.record_journal_entry();
    mgr.record_journal_entry();
    assert!(!mgr.should_auto_checkpoint());
    mgr.record_journal_entry();
    assert!(mgr.should_auto_checkpoint());

    cleanup(&dir);
}

// ------------------------------------------------------------------
// Schema Migration
// ------------------------------------------------------------------

#[test]
fn migration_chain_applies_in_order() -> SaveResult<()> {
    let mut pipeline = MigrationPipeline::new("1.2.0");
    pipeline.register(MigrationStep::new(
        "1.0.0",
        "1.1.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let idx = table.columns.len();
                table.column_index.insert("health".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("health", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));
    pipeline.register(MigrationStep::new(
        "1.1.0",
        "1.2.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let idx = table.columns.len();
                table.column_index.insert("stamina".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("stamina", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));

    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let migrated = pipeline.apply(&manifest)?;
    assert_eq!(migrated_target_version(&migrated), "1.2.0");
    let actors = migrated
        .manifest()
        .get_table("actors")
        .ok_or_else(|| SaveError::Generic("missing actors".to_owned()))?;
    assert!(actors.column_by_name("health").is_some());
    assert!(actors.column_by_name("stamina").is_some());
    Ok(())
}

#[test]
fn registry_routes_to_correct_pipeline() -> SaveResult<()> {
    let mut registry = MigrationRegistry::new();
    registry.register(MigrationPipeline::new("1.1.0"));

    let manifest = SchemaManifest::new("1.1.0").with_table(dummy_table("actors"));
    let migrated = registry.migrate(&manifest, "1.1.0")?;
    assert_eq!(migrated_target_version(&migrated), "1.1.0");
    Ok(())
}

// ------------------------------------------------------------------
// Mod Tolerance
// ------------------------------------------------------------------

#[test]
fn tolerance_exact_match() -> SaveResult<()> {
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0", "abc"));
    let available = vec![fp("base", "1.0", "abc")];

    let checker = ModToleranceChecker::default();
    let outcome = checker.check(&stored, &available)?;
    assert_eq!(outcome, ModLoadOutcome::ExactMatch);
    Ok(())
}

#[test]
fn tolerance_missing_mod_degrades() -> SaveResult<()> {
    let mut stored = FingerprintRegistry::new();
    let mut missing = fp("extra", "1.0", "abc");
    missing.table_specs.push("bonus_table".to_owned());
    stored.register(missing);

    let available: Vec<ModFingerprint> = vec![];
    let checker = ModToleranceChecker::default();
    let outcome = checker.check(&stored, &available)?;
    match outcome {
        ModLoadOutcome::MissingMods {
            degraded_tables, ..
        } => {
            assert!(degraded_tables.contains(&"bonus_table".to_owned()));
        }
        other => panic!("expected MissingMods, got {:?}", other),
    }
    Ok(())
}

#[test]
fn tolerance_extra_mods_allowed() -> SaveResult<()> {
    let stored = FingerprintRegistry::new();
    let available = vec![fp("extra", "1.0", "xyz")];

    let checker = ModToleranceChecker::default();
    let outcome = checker.check(&stored, &available)?;
    match outcome {
        ModLoadOutcome::ExtraMods { extra } => {
            assert_eq!(extra.len(), 1);
        }
        other => panic!("expected ExtraMods, got {:?}", other),
    }
    Ok(())
}

#[test]
fn tolerance_critical_missing_rejected() {
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0", "abc"));
    let available: Vec<ModFingerprint> = vec![];

    let checker = ModToleranceChecker::default().with_critical_mod("base");
    let result = checker.check(&stored, &available);
    assert!(matches!(result, Err(SaveError::CriticalModMissing(_))));
}

// ------------------------------------------------------------------
// Load Reconstruction (Six-Phase)
// ------------------------------------------------------------------

#[test]
fn load_reconstruction_builder_success() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let recon = LoadReconstructionBuilder::new()
        .persistence(persistence)
        .tolerance_checker(ModToleranceChecker::default())
        .build()?;

    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase1_reads_latest_snapshot() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(7), 0x1234));
    persistence.write_snapshot(&snapshot)?;

    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    let read_manifest = recon.phase1_snapshot_deserialize()?;
    assert_eq!(read_manifest.schema_version, "1.0.0");

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase1_no_snapshot_fails() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    let result = recon.phase1_snapshot_deserialize();
    assert!(matches!(result, Err(SaveError::SnapshotNotFound(_))));

    cleanup(&dir);
}

#[test]
fn phase2_produces_final_mods() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0", "abc"));
    let available = vec![fp("base", "1.0", "abc"), fp("extra", "1.0", "xyz")];

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    recon.skip_to_phase(LoadPhase::SnapshotDeserialize)?;
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;
    assert_eq!(final_mods.len(), 2);

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase3_with_pipeline_migrates() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

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

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default())
        .with_migration_pipeline(pipeline);
    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let migrated = recon.phase3_schema_migration(&manifest)?;
    assert_eq!(migrated_target_version(&migrated), "1.1.0");

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase5_freezes_lifecycle() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    recon.skip_to_phase(LoadPhase::ContentCompilation)?;
    recon.phase5_schema_freeze()?;
    assert!(recon.lifecycle().is_frozen());

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase6_finishes_lifecycle() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    recon.skip_to_phase(LoadPhase::ContentCompilation)?;
    recon.phase5_schema_freeze()?;
    recon.phase6_simulation_start()?;
    assert!(recon.lifecycle().is_frozen());
    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

#[test]
fn replay_diffs_computes_hash() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0");
    let header = SnapshotHeader::new(manifest, Tick(0), 0);
    let snapshot = PersistedSnapshot::new(header);
    persistence.write_snapshot(&snapshot)?;
    let recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    let records = vec![
        CommitRecord::new(
            Tick(1),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: scharnhorst_core::RowId::new(1),
            }],
            10,
        ),
        CommitRecord::new(
            Tick(2),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: scharnhorst_core::RowId::new(2),
            }],
            20,
        ),
    ];

    let hash = recon.replay_diffs(Tick(0), &records)?;
    assert_ne!(hash, 0);

    cleanup(&dir);
    Ok(())
}

#[test]
fn verify_state_hash_detects_mismatch() {
    let result = LoadReconstruction::verify_state_hash(1, 2);
    assert!(matches!(
        result,
        Err(SaveError::StateHashMismatch {
            expected: 2,
            got: 1
        })
    ));
}

// ------------------------------------------------------------------
// Integration: full save -> load roundtrip
// ------------------------------------------------------------------

#[test]
fn full_save_and_list_snapshots() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"));

    for gen in [100u64, 200, 300] {
        let snapshot =
            PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(gen), gen))
                .with_table_data("actors", vec![gen as u8])
                .with_table_data("provinces", vec![(gen / 10) as u8]);
        persistence.write_snapshot(&snapshot)?;
    }

    let list = persistence.list_snapshots()?;
    assert_eq!(list.len(), 3);

    let loaded = persistence.read_snapshot(Tick(200))?;
    assert_eq!(loaded.header.generation, Tick(200));
    assert_eq!(loaded.table_data.len(), 2);

    cleanup(&dir);
    Ok(())
}

// ------------------------------------------------------------------
// Edge case tests
// ------------------------------------------------------------------

#[test]
fn load_with_corrupt_manifest_returns_error() {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let persistence = SnapshotPersistence::new(&dir);
    let corrupt_path = persistence.snapshot_path(Tick(1));

    // Write a snapshot file with invalid JSON in the header
    use std::io::Write;
    let mut file = std::fs::File::create(&corrupt_path).unwrap();
    let fake_header_len: u64 = 7;
    file.write_all(&fake_header_len.to_le_bytes()).unwrap();
    file.write_all(b"NOTJSON").unwrap();
    file.write_all(&0u64.to_le_bytes()).unwrap(); // zero tables

    let result = persistence.read_snapshot(Tick(1));
    assert!(
        matches!(result, Err(SaveError::IpcDeserialization(_))),
        "expected IpcDeserialization"
    );

    cleanup(&dir);
}

#[test]
fn load_phase_out_of_order_returns_error() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(7), 0x1234));
    persistence.write_snapshot(&snapshot)?;

    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    // Run phase 1
    recon.phase1_snapshot_deserialize()?;

    // Attempt phase 3 directly, skipping phase 2 — should fail
    let result = recon.phase3_schema_migration(&manifest);
    assert!(
        matches!(result, Err(SaveError::InvalidPhaseTransition { .. })),
        "expected InvalidPhaseTransition, got {:?}",
        result
    );

    cleanup(&dir);
    Ok(())
}

#[test]
fn replay_diffs_with_no_snapshot_returns_error() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    let records: Vec<CommitRecord> = vec![];
    let result = recon.replay_diffs(Tick(999), &records);
    assert!(
        matches!(result, Err(SaveError::SnapshotNotFound(_))),
        "expected SnapshotNotFound, got {:?}",
        result
    );

    cleanup(&dir);
}

#[test]
fn mod_tolerance_strict_rejects_missing_mod() {
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0", "abc"));
    stored.register(fp("dlc", "1.0", "def"));

    // Available mods don't include "dlc"
    let available = vec![fp("base", "1.0", "abc")];

    let checker = ModToleranceChecker::new(ModTolerancePolicy::Strict);
    let result = checker.check(&stored, &available);
    assert!(
        result.is_err(),
        "Strict policy should reject missing mod, got {:?}",
        result
    );

    let err = result.unwrap_err();
    assert!(
        matches!(err, SaveError::Generic(_)),
        "expected Generic error about missing mod, got {:?}",
        err
    );
}
