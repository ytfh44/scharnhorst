//! Six-Phase Load Lifecycle Tests
//!
//! This test suite validates the complete six-phase load reconstruction lifecycle
//! as specified in the save-system specification.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use scharnhorst_arrow_store::{ArrowStore, InitStore};
use scharnhorst_content::{
    migrated_target_version, FingerprintRegistry, LoadPhase, ModFingerprint, SchemaManifest,
};
use scharnhorst_core::{RowId, Tick};
use scharnhorst_journal::{CommitRecord, Diff};
use scharnhorst_save::{
    LoadReconstruction, LoadReconstructionBuilder, MigrationPipeline, MigrationRegistry,
    MigrationStep, ModToleranceChecker, ModTolerancePolicy, PersistedSnapshot, SaveError,
    SaveResult, SnapshotHeader, SnapshotPersistence,
};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, RelationEdge, RelationKind, TableSpec};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("sch_six_phase_test_{}_{}", std::process::id(), id))
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

fn fp_with_tables(mod_id: &str, version: &str, hash: &str, tables: &[&str]) -> ModFingerprint {
    let mut fingerprint = ModFingerprint::new(mod_id, version).with_content_hash(hash);
    for table in tables {
        fingerprint.table_specs.push(table.to_string());
    }
    fingerprint
}

fn make_init_store() -> InitStore {
    InitStore::new(Arc::new(ArrowStore::new()))
}

// ===================================================================================
// Phase 1: Snapshot Deserialize Tests
// ===================================================================================

#[test]
fn phase1_deserialize_extracts_schema_manifest() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Create a snapshot with a specific schema manifest
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"))
        .with_mod_fingerprint(fp("base", "1.0.0", "abc123"));

    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(100), 0x1234))
        .with_table_data("actors", vec![1, 2, 3])
        .with_table_data("provinces", vec![4, 5, 6]);

    persistence.write_snapshot(&snapshot)?;

    // Phase 1: Deserialize snapshot header
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    let extracted_manifest = recon.phase1_snapshot_deserialize()?;

    // Verify extracted manifest matches original
    assert_eq!(extracted_manifest.schema_version, "1.0.0");
    assert_eq!(extracted_manifest.tables.len(), 2);
    assert_eq!(extracted_manifest.mod_fingerprints.len(), 1);

    // Verify lifecycle state
    assert!(recon.lifecycle().manifest().is_some());
    assert_eq!(
        recon.lifecycle().manifest().unwrap().schema_version,
        "1.0.0"
    );

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase1_extracts_mod_fingerprints() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Create manifest with multiple mod fingerprints
    let manifest = SchemaManifest::new("1.0.0")
        .with_mod_fingerprint(fp("base", "1.0.0", "base_hash"))
        .with_mod_fingerprint(fp("dlc1", "1.2.0", "dlc1_hash"))
        .with_mod_fingerprint(fp("mod_a", "0.5.0", "mod_a_hash"));

    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(50), 0x5678));
    persistence.write_snapshot(&snapshot)?;

    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());
    let extracted = recon.phase1_snapshot_deserialize()?;

    assert_eq!(extracted.mod_fingerprints.len(), 3);

    let ids: HashSet<&str> = extracted
        .mod_fingerprints
        .iter()
        .map(|f| f.mod_id.as_str())
        .collect();
    assert!(ids.contains("base"));
    assert!(ids.contains("dlc1"));
    assert!(ids.contains("mod_a"));

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase1_no_snapshot_returns_error() {
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
fn phase1_corrupted_snapshot_detected() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    // Write a corrupted snapshot file with a valid header length but invalid JSON
    // Header format: [header_len: u64][header_json][table_count: u64][tables...]
    let path = dir.join("snapshot_gen_1.arrow");

    // Create a file with a small header length that points to invalid JSON
    let mut file_data = Vec::new();
    // Header length = 5 (small value)
    file_data.extend_from_slice(&5u64.to_le_bytes());
    // Invalid JSON (5 bytes)
    file_data.extend_from_slice(b"{bad");

    std::fs::write(&path, file_data).unwrap();

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    // Should fail when trying to parse the corrupted header
    let result = recon.phase1_snapshot_deserialize();
    assert!(result.is_err());

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Phase 2: Mod Coordination Tests
// ===================================================================================

#[test]
fn phase2_exact_match_produces_final_mods() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "hash1"));
    stored.register(fp("mod_a", "1.0.0", "hash2"));

    let available = vec![fp("base", "1.0.0", "hash1"), fp("mod_a", "1.0.0", "hash2")];

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize)?;
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;

    assert_eq!(final_mods.len(), 2);
    assert!(recon.lifecycle().final_mods().len() == 2);

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase2_missing_mod_degrades_tables() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp_with_tables(
        "bonus_mod",
        "1.0.0",
        "hash1",
        &["bonus_table1", "bonus_table2"],
    ));

    // Available mods don't include bonus_mod
    let available: Vec<ModFingerprint> = vec![];

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize)?;
    // Should succeed but mark tables as degraded
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;

    // Final mods should still include reference to the missing mod for record-keeping
    assert_eq!(final_mods.len(), 1);

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase2_extra_mod_included_in_compilation() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    // Save has only base mod
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "hash1"));

    // But disk has extra mod
    let available = vec![
        fp("base", "1.0.0", "hash1"),
        fp("extra_mod", "2.0.0", "hash2"),
    ];

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize)?;
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;

    // Extra mod should be included in final list
    assert_eq!(final_mods.len(), 2);
    let ids: Vec<&str> = final_mods.iter().map(|f| f.mod_id.as_str()).collect();
    assert!(ids.contains(&"base"));
    assert!(ids.contains(&"extra_mod"));

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase2_version_mismatch_warns_but_continues() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("mod_a", "1.0.0", "hash1"));

    // Available has different version
    let available = vec![fp("mod_a", "1.1.0", "hash2")];

    let checker = ModToleranceChecker::new(ModTolerancePolicy::Warn);
    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, checker);

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize)?;
    // Should succeed with warning (policy is Warn)
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;
    assert_eq!(final_mods.len(), 1);
    assert_eq!(final_mods[0].version, "1.1.0");

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase2_critical_mod_missing_rejects_load() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base_game", "1.0.0", "hash1"));

    // No available mods
    let available: Vec<ModFingerprint> = vec![];

    let checker = ModToleranceChecker::default().with_critical_mod("base_game");
    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, checker);

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize).ok();
    let result = recon.phase2_mod_coordination(&stored, &available);
    assert!(matches!(result, Err(SaveError::CriticalModMissing(_))));

    cleanup(&dir);
}

#[test]
fn phase4_rejects_cyclic_relations() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("directors"))
        .with_relation(RelationEdge {
            from: "actors".to_string(),
            to: "directors".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: Some("id".to_string()),
        })
        .with_relation(RelationEdge {
            from: "directors".to_string(),
            to: "actors".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: Some("id".to_string()),
        });
    recon.phase3_schema_migration(&manifest)?;

    let init_store = make_init_store();
    let result = recon.phase4_content_compilation_entry(&init_store);
    assert!(matches!(result, Err(SaveError::Schema(_))),
        "cyclic relations should be rejected by detect_cycles or add_edge");

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase4_accepts_non_cyclic_dag() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("countries"))
        .with_table(dummy_table("provinces"))
        .with_table(dummy_table("cities"))
        .with_relation(RelationEdge {
            from: "countries".to_string(),
            to: "provinces".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: Some("id".to_string()),
        })
        .with_relation(RelationEdge {
            from: "provinces".to_string(),
            to: "cities".to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: Some("id".to_string()),
        });
    recon.phase3_schema_migration(&manifest)?;

    let init_store = make_init_store();
    recon.phase4_content_compilation_entry(&init_store)?;

    assert_eq!(recon.tier_registry().table_count(), 3);
    assert!(recon.tier_registry().is_authority("countries"));
    assert!(recon.tier_registry().is_authority("provinces"));
    assert!(recon.tier_registry().is_authority("cities"));

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Phase 3: Schema Migration Tests
// ===================================================================================

#[test]
fn phase3_no_migration_needed() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));

    let migrated = recon.phase3_schema_migration(&manifest)?;

    assert_eq!(migrated_target_version(&migrated), "1.0.0");
    assert_eq!(migrated.tables().len(), 1);

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase3_missing_migration_path_fails() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    // Pipeline only knows how to migrate to 2.0.0
    let pipeline = MigrationPipeline::new("2.0.0");

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default())
        .with_migration_pipeline(pipeline);

    recon.skip_to_phase(LoadPhase::ModCoordination).ok();
    // But manifest is at 1.0.0
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));

    let result = recon.phase3_schema_migration(&manifest);
    assert!(matches!(result, Err(SaveError::NoMigrationPath(_))));

    cleanup(&dir);
}

// ===================================================================================
// Phase 4: Content Compilation Entry Tests
// ===================================================================================

#[test]
fn phase4_entry_returns_migrated_manifest() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    // First run phase 3 to set the migrated manifest
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let _ = recon.phase3_schema_migration(&manifest)?;

    // Phase 4: create Authority tables via InitStore
    let init_store = make_init_store();
    recon.phase4_content_compilation_entry(&init_store)?;
    assert_eq!(recon.tier_registry().table_count(), 1);
    assert!(recon.tier_registry().is_authority("actors"));

    cleanup(&dir);
    Ok(())
}

#[test]
fn phase4_entry_fails_without_phase3() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::SchemaMigration).ok();
    // Phase 4 entry without running phase 3 should fail
    let init_store = make_init_store();
    let result = recon.phase4_content_compilation_entry(&init_store);
    assert!(matches!(result, Err(SaveError::Generic(_))));

    cleanup(&dir);
}

// ===================================================================================
// Phase 5: Schema Freeze Tests
// ===================================================================================

#[test]
fn phase5_freezes_schema_registry() -> SaveResult<()> {
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
fn phase3_migration_preserves_relations() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut pipeline = MigrationPipeline::new("1.1.0");
    pipeline.register(MigrationStep::new(
        "1.0.0",
        "1.1.0",
        |_table: &mut TableSpec| Ok(()),
    ));

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default())
        .with_migration_pipeline(pipeline);

    recon.skip_to_phase(LoadPhase::ModCoordination)?;
    let relation = RelationEdge {
        from: "actors".to_owned(),
        to: "provinces".to_owned(),
        kind: RelationKind::OneToMany,
        from_column: "province_id".to_owned(),
        to_column: Some("id".to_owned()),
    };

    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"))
        .with_relation(relation);

    let migrated = recon.phase3_schema_migration(&manifest)?;

    assert_eq!(migrated.relations().len(), 1);
    assert_eq!(migrated.relations()[0].from, "actors");
    assert_eq!(migrated.relations()[0].to, "provinces");

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Phase 6: Simulation Start Tests
// ===================================================================================

#[test]
fn phase6_finishes_lifecycle() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ContentCompilation)?;
    // Must freeze before starting simulation
    recon.phase5_schema_freeze()?;
    recon.phase6_simulation_start()?;

    assert!(recon.lifecycle().is_frozen());
    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Full Six-Phase Lifecycle Integration Tests
// ===================================================================================

#[test]
fn full_six_phase_lifecycle_success() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    // Setup: Create a snapshot file
    let persistence = SnapshotPersistence::new(&dir);

    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"))
        .with_mod_fingerprint(fp("base", "1.0.0", "base_hash"));

    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 0x1234))
        .with_table_data("actors", vec![1, 2, 3])
        .with_table_data("provinces", vec![4, 5, 6]);

    persistence.write_snapshot(&snapshot)?;

    // Setup mod coordination
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "base_hash"));
    let available = vec![fp("base", "1.0.0", "base_hash")];

    // Setup migration pipeline
    let mut pipeline = MigrationPipeline::new("1.1.0");
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

    // Create load reconstruction
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default())
        .with_migration_pipeline(pipeline);

    // Phase 1: Snapshot Deserialize
    let manifest = recon.phase1_snapshot_deserialize()?;
    assert_eq!(manifest.schema_version, "1.0.0");

    // Phase 2: Mod Coordination
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;
    assert_eq!(final_mods.len(), 1);

    // Phase 3: Schema Migration
    let migrated = recon.phase3_schema_migration(&manifest)?;
    assert_eq!(migrated_target_version(&migrated), "1.1.0");
    assert!(migrated
        .manifest()
        .get_table("actors")
        .unwrap()
        .column_by_name("health")
        .is_some());

    // Phase 4: Content Compilation Entry
    let init_store = make_init_store();
    recon.phase4_content_compilation_entry(&init_store)?;
    assert!(recon.tier_registry().is_authority("actors"));
    assert!(recon.tier_registry().is_authority("provinces"));

    // Phase 5: Schema Freeze
    recon.phase5_schema_freeze()?;
    assert!(recon.lifecycle().is_frozen());

    // Phase 6: Simulation Start
    recon.phase6_simulation_start()?;
    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

#[test]
fn full_lifecycle_with_journal_replay() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Create snapshot at generation 100
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 0x1000));
    persistence.write_snapshot(&snapshot)?;

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "hash"));
    let available = vec![fp("base", "1.0.0", "hash")];

    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    // Run phases 1-3
    let manifest = recon.phase1_snapshot_deserialize()?;
    let _ = recon.phase2_mod_coordination(&stored, &available)?;
    let _ = recon.phase3_schema_migration(&manifest)?;

    // Simulate journal replay: diffs from tick 101 to 105
    let records: Vec<CommitRecord> = (101u64..=105)
        .map(|tick| {
            CommitRecord::new(
                Tick(tick),
                vec![Diff::Delete {
                    table: "actors".to_owned(),
                    row: RowId::new(tick),
                }],
                tick,
            )
        })
        .collect();

    let final_hash = recon.replay_diffs(Tick(100), &records)?;

    // Hash should be deterministic
    assert_ne!(final_hash, 0);

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Error Scenario Tests
// ===================================================================================

#[test]
fn state_hash_mismatch_detects_corruption() {
    let result = LoadReconstruction::verify_state_hash(0x1234, 0x5678);
    assert!(matches!(
        result,
        Err(SaveError::StateHashMismatch {
            expected: 0x5678,
            got: 0x1234
        })
    ));
}

#[test]
fn state_hash_match_succeeds() -> SaveResult<()> {
    LoadReconstruction::verify_state_hash(0x1234, 0x1234)?;
    Ok(())
}

#[test]
fn base_game_mod_missing_rejects_load() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base_game", "1.0.0", "hash1"));
    stored.register(fp("dlc", "1.0.0", "hash2"));

    // Available mods don't include base_game
    let available = vec![fp("dlc", "1.0.0", "hash2")];

    let checker = ModToleranceChecker::default().with_critical_mod("base_game");

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, checker);

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize).ok();
    let result = recon.phase2_mod_coordination(&stored, &available);
    assert!(matches!(result, Err(SaveError::CriticalModMissing(_))));

    cleanup(&dir);
}

#[test]
fn strict_policy_rejects_any_mismatch() {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("mod_a", "1.0.0", "hash1"));

    // Version mismatch
    let available = vec![fp("mod_a", "1.1.0", "hash2")];

    let checker = ModToleranceChecker::new(ModTolerancePolicy::Strict);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, checker);

    recon.skip_to_phase(LoadPhase::SnapshotDeserialize).ok();
    // Strict policy should reject version mismatch
    let result = recon.phase2_mod_coordination(&stored, &available);
    assert!(result.is_err());

    cleanup(&dir);
}

// ===================================================================================
// Migration Registry Integration Tests
// ===================================================================================

#[test]
fn migration_registry_routes_to_correct_pipeline() -> SaveResult<()> {
    let mut registry = MigrationRegistry::new();

    // Register a pipeline that can migrate from 1.0.0 to 1.2.0 in two steps
    let mut pipeline_120 = MigrationPipeline::new("1.2.0");

    // Step 1: 1.0.0 -> 1.1.0
    pipeline_120.register(MigrationStep::new(
        "1.0.0",
        "1.1.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let idx = table.columns.len();
                table.column_index.insert("field_110".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("field_110", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));

    // Step 2: 1.1.0 -> 1.2.0
    pipeline_120.register(MigrationStep::new(
        "1.1.0",
        "1.2.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let idx = table.columns.len();
                table.column_index.insert("field_120".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("field_120", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));

    registry.register(pipeline_120);

    // Migrate from 1.0.0 to 1.2.0
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let migrated = registry.migrate(&manifest, "1.2.0")?;

    assert_eq!(migrated_target_version(&migrated), "1.2.0");

    // Both migrations should have been applied
    let actors = migrated
        .manifest()
        .get_table("actors")
        .ok_or_else(|| SaveError::Generic("missing actors".to_owned()))?;
    assert!(actors.column_by_name("field_110").is_some());
    assert!(actors.column_by_name("field_120").is_some());

    Ok(())
}

#[test]
fn migration_registry_unknown_target_fails() {
    let registry = MigrationRegistry::new();
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));

    let result = registry.migrate(&manifest, "9.9.9");
    assert!(matches!(result, Err(SaveError::NoMigrationPath(_))));
}

// ===================================================================================
// LoadReconstructionBuilder Tests
// ===================================================================================

#[test]
fn builder_requires_persistence() {
    let result = LoadReconstructionBuilder::new()
        .tolerance_checker(ModToleranceChecker::default())
        .build();
    assert!(result.is_err());
}

#[test]
fn builder_requires_tolerance_checker() {
    let dir = temp_dir();
    let persistence = SnapshotPersistence::new(&dir);

    let result = LoadReconstructionBuilder::new()
        .persistence(persistence)
        .build();
    assert!(result.is_err());
}

#[test]
fn builder_full_configuration() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let checker = ModToleranceChecker::default();
    let pipeline = MigrationPipeline::new("1.1.0");

    let recon = LoadReconstructionBuilder::new()
        .persistence(persistence)
        .tolerance_checker(checker)
        .migration_pipeline(pipeline)
        .build()?;

    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Journal Replay and State Hash Tests
// ===================================================================================

#[test]
fn replay_empty_diffs_returns_zero_hash() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0");
    let header = SnapshotHeader::new(manifest, Tick(0), 0);
    let snapshot = PersistedSnapshot::new(header);
    persistence.write_snapshot(&snapshot)?;

    let recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    let hash = recon.replay_diffs(Tick(0), &[])?;
    assert_eq!(hash, 0);

    cleanup(&dir);
    Ok(())
}

#[test]
fn replay_diffs_computes_deterministic_hash() -> SaveResult<()> {
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
                row: RowId::new(1),
            }],
            10,
        ),
        CommitRecord::new(
            Tick(2),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: RowId::new(2),
            }],
            20,
        ),
    ];

    let hash1 = recon.replay_diffs(Tick(0), &records)?;
    let hash2 = recon.replay_diffs(Tick(0), &records)?;

    // Hash should be deterministic
    assert_eq!(hash1, hash2);
    assert_ne!(hash1, 0);

    cleanup(&dir);
    Ok(())
}

#[test]
fn replay_diffs_filters_by_base_tick() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let manifest = SchemaManifest::new("1.0.0");
    let header0 = SnapshotHeader::new(manifest.clone(), Tick(0), 0);
    let header10 = SnapshotHeader::new(manifest, Tick(10), 100);
    persistence.write_snapshot(&PersistedSnapshot::new(header0))?;
    persistence.write_snapshot(&PersistedSnapshot::new(header10))?;
    let recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    let records = vec![
        CommitRecord::new(
            Tick(5),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: RowId::new(1),
            }],
            10,
        ),
        CommitRecord::new(
            Tick(10),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: RowId::new(2),
            }],
            20,
        ),
        CommitRecord::new(
            Tick(15),
            vec![Diff::Delete {
                table: "actors".to_owned(),
                row: RowId::new(3),
            }],
            30,
        ),
    ];

    // Only diffs with tick > 10 should be applied
    let hash = recon.replay_diffs(Tick(10), &records)?;

    // Hash should be different from replaying all diffs
    let hash_all = recon.replay_diffs(Tick(0), &records)?;
    assert_ne!(hash, hash_all);

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Lifecycle State Tracking Tests
// ===================================================================================

#[test]
fn lifecycle_tracks_completed_phases() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Create a snapshot
    let manifest = SchemaManifest::new("1.0.0").with_table(dummy_table("actors"));
    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest.clone(), Tick(1), 0x1));
    persistence.write_snapshot(&snapshot)?;

    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "hash"));
    let available = vec![fp("base", "1.0.0", "hash")];

    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    // Initially no phases completed
    assert!(recon.lifecycle().completed_phases().is_empty());

    // After phase 1
    let _ = recon.phase1_snapshot_deserialize()?;
    // Note: phases are marked completed when advancing to next

    // After phase 2
    let _ = recon.phase2_mod_coordination(&stored, &available)?;

    // After phase 3
    let _ = recon.phase3_schema_migration(&manifest)?;

    // Verify lifecycle state
    assert!(recon.lifecycle().manifest().is_some());
    assert!(recon.lifecycle().migrated_manifest().is_some());
    assert!(!recon.lifecycle().final_mods().is_empty());

    cleanup(&dir);
    Ok(())
}

#[test]
fn lifecycle_is_finished_after_phase6() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let persistence = SnapshotPersistence::new(&dir);
    let mut recon = LoadReconstruction::new(persistence, ModToleranceChecker::default());

    recon.skip_to_phase(LoadPhase::ContentCompilation)?;
    // Run through phases 5 and 6
    recon.phase5_schema_freeze()?;
    recon.phase6_simulation_start()?;

    // After phase 6, lifecycle should indicate completion
    assert!(recon.lifecycle().current_phase().is_none());

    cleanup(&dir);
    Ok(())
}

// ===================================================================================
// Complex Scenario Tests
// ===================================================================================

#[test]
fn load_with_multiple_mods_and_migrations() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Create snapshot with multiple mods
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_table(dummy_table("provinces"))
        .with_table(dummy_table("factions"))
        .with_mod_fingerprint(fp("base", "1.0.0", "base_hash"))
        .with_mod_fingerprint(fp("dlc_units", "1.0.0", "dlc_hash"))
        .with_mod_fingerprint(fp("mod_graphics", "2.0.0", "gfx_hash"));

    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(1000), 0xdeadbeef));
    persistence.write_snapshot(&snapshot)?;

    // Stored mods
    let mut stored = FingerprintRegistry::new();
    stored.register(fp("base", "1.0.0", "base_hash"));
    stored.register(fp("dlc_units", "1.0.0", "dlc_hash"));
    stored.register(fp("mod_graphics", "2.0.0", "gfx_hash"));

    // Available mods (dlc_units has newer version, extra_mod is new)
    let available = vec![
        fp("base", "1.0.0", "base_hash"),
        fp("dlc_units", "1.1.0", "dlc_new_hash"), // version mismatch
        fp("mod_graphics", "2.0.0", "gfx_hash"),
        fp("extra_mod", "1.0.0", "extra_hash"), // extra mod
    ];

    // Complex migration pipeline
    let mut pipeline = MigrationPipeline::new("1.2.0");

    // 1.0.0 -> 1.1.0: Add experience column to actors
    pipeline.register(MigrationStep::new(
        "1.0.0",
        "1.1.0",
        |table: &mut TableSpec| {
            if table.name == "actors" {
                let idx = table.columns.len();
                table.column_index.insert("experience".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("experience", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));

    // 1.1.0 -> 1.2.0: Add reputation column to factions
    pipeline.register(MigrationStep::new(
        "1.1.0",
        "1.2.0",
        |table: &mut TableSpec| {
            if table.name == "factions" {
                let idx = table.columns.len();
                table.column_index.insert("reputation".to_owned(), idx);
                table
                    .columns
                    .push(ColumnSpec::new("reputation", FieldSemantic::Raw, "i64"));
            }
            Ok(())
        },
    ));

    let checker = ModToleranceChecker::default();
    let mut recon = LoadReconstruction::new(persistence, checker).with_migration_pipeline(pipeline);

    // Execute all phases
    let manifest = recon.phase1_snapshot_deserialize()?;
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;
    let migrated = recon.phase3_schema_migration(&manifest)?;

    // Verify results
    assert_eq!(final_mods.len(), 4); // All mods including extra
    assert_eq!(migrated_target_version(&migrated), "1.2.0");

    // Verify migrations were applied
    let actors = migrated
        .manifest()
        .get_table("actors")
        .ok_or_else(|| SaveError::Generic("missing actors".to_owned()))?;
    let factions = migrated
        .manifest()
        .get_table("factions")
        .ok_or_else(|| SaveError::Generic("missing factions".to_owned()))?;

    assert!(actors.column_by_name("experience").is_some());
    assert!(factions.column_by_name("reputation").is_some());

    // Complete remaining phases
    let init_store = make_init_store();
    recon.phase4_content_compilation_entry(&init_store)?;
    recon.phase5_schema_freeze()?;
    recon.phase6_simulation_start()?;

    cleanup(&dir);
    Ok(())
}

#[test]
fn load_reconstruction_with_missing_non_critical_mod() -> SaveResult<()> {
    let dir = temp_dir();
    cleanup(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

    let persistence = SnapshotPersistence::new(&dir);

    // Save has optional cosmetic mod
    let manifest = SchemaManifest::new("1.0.0")
        .with_table(dummy_table("actors"))
        .with_mod_fingerprint(fp_with_tables(
            "cosmetic_skins",
            "1.0.0",
            "hash",
            &["skin_overrides"],
        ));

    let snapshot = PersistedSnapshot::new(SnapshotHeader::new(manifest, Tick(100), 0x1234));
    persistence.write_snapshot(&snapshot)?;

    let mut stored = FingerprintRegistry::new();
    stored.register(fp_with_tables(
        "cosmetic_skins",
        "1.0.0",
        "hash",
        &["skin_overrides"],
    ));

    // Cosmetic mod is missing but not critical
    let available: Vec<ModFingerprint> = vec![];

    let checker = ModToleranceChecker::default(); // No critical mods
    let mut recon = LoadReconstruction::new(persistence, checker);

    let _ = recon.phase1_snapshot_deserialize()?;

    // Should succeed with degraded tables
    let final_mods = recon.phase2_mod_coordination(&stored, &available)?;
    assert_eq!(final_mods.len(), 1); // Still includes the missing mod reference

    cleanup(&dir);
    Ok(())
}
