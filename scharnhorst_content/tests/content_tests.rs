use std::collections::HashMap;

use scharnhorst_content::compilation::{CompilationInput, ContentCompiler, RawTableDef};
use scharnhorst_content::error::ContentError;
use scharnhorst_content::fingerprint::{FingerprintRegistry, ModFingerprintExt};
use scharnhorst_content::lifecycle::{LifecycleCoordinator, LoadLifecycle, LoadPhase};
use scharnhorst_content::manifest::{
    manifest_from_bytes, manifest_to_bytes,
    migrated_from_manifest, migrated_table_by_name, migrated_target_version,
};
use scharnhorst_content::name_resolution::{NameResolver, NamespaceResolver};
use scharnhorst_content::overlay::{MergeStrategy, OverlayEntry, OverlayLayer, OverlayResolver};
use scharnhorst_content::ModFingerprint;
use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};
use scharnhorst_schema::{ColumnSpec, FieldSemantic, TableSpec};

// ------------------------------------------------------------------
// Overlay resolver tests
// ------------------------------------------------------------------

#[test]
fn overlay_resolver_base_only() {
    let resolver = OverlayResolver::new().with_base("actor.FRA.stability", "50");
    assert_eq!(resolver.resolve("actor.FRA.stability").unwrap(), "50");
}

#[test]
fn overlay_resolver_replace() {
    let mut resolver = OverlayResolver::new().with_base("actor.FRA.stability", "50");
    let layer = OverlayLayer::new("mod_a", 1).with_entry(OverlayEntry {
        path: "actor.FRA.stability".to_owned(),
        value: "75".to_owned(),
        strategy: MergeStrategy::Replace,
    });
    resolver.add_layer(layer);
    assert_eq!(resolver.resolve("actor.FRA.stability").unwrap(), "75");
}

#[test]
fn overlay_resolver_add() {
    let mut resolver = OverlayResolver::new().with_base("actor.FRA.stability", "50");
    let layer = OverlayLayer::new("mod_a", 1).with_entry(OverlayEntry {
        path: "actor.FRA.stability".to_owned(),
        value: "10".to_owned(),
        strategy: MergeStrategy::Add,
    });
    resolver.add_layer(layer);
    assert_eq!(resolver.resolve("actor.FRA.stability").unwrap(), "60");
}

#[test]
fn overlay_resolver_priority_order() {
    let mut resolver = OverlayResolver::new().with_base("actor.FRA.stability", "50");
    let low = OverlayLayer::new("low", 1).with_entry(OverlayEntry {
        path: "actor.FRA.stability".to_owned(),
        value: "60".to_owned(),
        strategy: MergeStrategy::Replace,
    });
    let high = OverlayLayer::new("high", 2).with_entry(OverlayEntry {
        path: "actor.FRA.stability".to_owned(),
        value: "70".to_owned(),
        strategy: MergeStrategy::Replace,
    });
    resolver.add_layer(low);
    resolver.add_layer(high);
    assert_eq!(resolver.resolve("actor.FRA.stability").unwrap(), "70");
}

#[test]
fn overlay_resolver_not_found() {
    let resolver = OverlayResolver::new();
    let err = resolver.resolve("missing.path").unwrap_err();
    assert!(matches!(err, ContentError::OverlayNotFound(_)));
}

#[test]
fn overlay_resolver_known_paths() {
    let mut resolver = OverlayResolver::new().with_base("a", "1");
    let layer = OverlayLayer::new("mod", 1).with_entry(OverlayEntry {
        path: "b".to_owned(),
        value: "2".to_owned(),
        strategy: MergeStrategy::Replace,
    });
    resolver.add_layer(layer);
    let paths: Vec<&str> = resolver.known_paths().collect();
    assert_eq!(paths.len(), 2);
    assert!(paths.contains(&"a"));
    assert!(paths.contains(&"b"));
}

// ------------------------------------------------------------------
// Name resolution tests
// ------------------------------------------------------------------

#[test]
fn name_resolver_register_and_lookup() {
    let mut resolver = NameResolver::new();
    let id = resolver.register("FRA").unwrap();
    assert_eq!(resolver.resolve("FRA").unwrap(), id);
    assert_eq!(resolver.name_of(id).unwrap(), "FRA");
}

#[test]
fn name_resolver_duplicate() {
    let mut resolver = NameResolver::new();
    resolver.register("FRA").unwrap();
    let err = resolver.register("FRA").unwrap_err();
    assert!(matches!(err, ContentError::DuplicateName(_)));
}

#[test]
fn name_resolver_missing() {
    let resolver = NameResolver::new();
    let err = resolver.resolve("ENG").unwrap_err();
    assert!(matches!(err, ContentError::NameResolutionFailed(_)));
}

#[test]
fn name_resolver_ids_are_stable() {
    let mut resolver = NameResolver::new();
    let id_fra = resolver.register("FRA").unwrap();
    let id_eng = resolver.register("ENG").unwrap();
    assert_ne!(id_fra, id_eng);
    assert_eq!(resolver.resolve("FRA").unwrap(), id_fra);
}

#[test]
fn namespace_resolver_per_table() {
    let mut ns = NamespaceResolver::new();
    let actor_id = ns.namespace("actors").register("FRA").unwrap();
    ns.sync_next_id();
    let province_id = ns.namespace("provinces").register("Paris").unwrap();
    ns.sync_next_id();

    assert_ne!(actor_id, province_id);
    assert_eq!(ns.resolve("actors", "FRA").unwrap(), actor_id);
    assert_eq!(ns.name_of("actors", actor_id).unwrap(), "FRA");
}

#[test]
fn namespace_resolver_missing_table() {
    let ns = NamespaceResolver::new();
    let err = ns.resolve("missing", "x").unwrap_err();
    assert!(matches!(err, ContentError::NameResolutionFailed(_)));
}

// ------------------------------------------------------------------
// Fingerprint tests
// ------------------------------------------------------------------

#[test]
fn mod_fingerprint_builder() {
    let fp = ModFingerprint::new("EuropaBarbarorum", "v2.1")
        .with_table_spec("actors")
        .with_table_spec("provinces");
    assert_eq!(fp.mod_id, "EuropaBarbarorum");
    assert_eq!(fp.version, "v2.1");
    assert!(fp.table_specs.contains(&"actors".to_owned()));
}

#[test]
fn mod_fingerprint_compute_hash() {
    let mut fp = ModFingerprint::new("mod_a", "v1").with_table_spec("t1");
    fp.compute_hash("seed");
    assert!(!fp.content_hash.is_empty());
}

#[test]
fn fingerprint_registry_register_and_find() {
    let mut reg = FingerprintRegistry::new();
    let fp = ModFingerprint::new("mod_a", "v1");
    reg.register(fp.clone());
    assert!(reg.contains("mod_a"));
    assert_eq!(reg.find("mod_a").unwrap().version, "v1");
}

#[test]
fn fingerprint_registry_compare_exact_match() {
    let mut reg = FingerprintRegistry::new();
    let fp = ModFingerprint::new("mod_a", "v1").with_content_hash("abc");
    reg.register(fp.clone());

    let available = vec![fp];
    let comp = reg.compare(&available);
    assert!(comp.is_exact_match());
    assert!(comp.missing.is_empty());
    assert!(comp.extra.is_empty());
    assert!(comp.mismatched.is_empty());
}

#[test]
fn fingerprint_registry_compare_missing() {
    let mut reg = FingerprintRegistry::new();
    let fp = ModFingerprint::new("mod_a", "v1");
    reg.register(fp);

    let comp = reg.compare(&[]);
    assert!(!comp.is_exact_match());
    assert_eq!(comp.missing.len(), 1);
    assert_eq!(comp.missing[0].mod_id, "mod_a");
}

#[test]
fn fingerprint_registry_compare_extra() {
    let reg = FingerprintRegistry::new();
    let fp = ModFingerprint::new("mod_a", "v1");

    let available = vec![fp];
    let comp = reg.compare(&available);
    assert!(!comp.is_exact_match());
    assert_eq!(comp.extra.len(), 1);
}

#[test]
fn fingerprint_registry_compare_mismatched() {
    let mut reg = FingerprintRegistry::new();
    let stored = ModFingerprint::new("mod_a", "v1").with_content_hash("abc");
    reg.register(stored);

    let available = vec![ModFingerprint::new("mod_a", "v2").with_content_hash("def")];
    let comp = reg.compare(&available);
    assert!(!comp.is_exact_match());
    assert_eq!(comp.mismatched.len(), 1);
}

// ------------------------------------------------------------------
// Manifest tests
// ------------------------------------------------------------------

#[test]
fn schema_manifest_roundtrip() {
    let spec = TableSpec::new("actors");
    let manifest = SchemaManifest::new("0.1.0")
        .with_metadata("engine_version", "0.1.0")
        .with_table(spec.clone());
    let bytes = manifest_to_bytes(&manifest).unwrap();
    let restored = manifest_from_bytes(&bytes).unwrap();
    assert_eq!(manifest.schema_version, restored.schema_version);
    assert_eq!(manifest.tables.len(), restored.tables.len());
}

#[test]
fn migrated_schema_manifest_from_manifest() {
    let spec = TableSpec::new("actors");
    let manifest = SchemaManifest::new("0.1.0")
        .with_metadata("engine_version", "0.1.0")
        .with_table(spec.clone());
    let migrated = migrated_from_manifest(&manifest, "0.2.0");
    assert_eq!(migrated_target_version(&migrated), "0.2.0");
    assert_eq!(migrated.tables().len(), 1);
}

#[test]
fn migrated_schema_manifest_table_by_name() {
    let mut manifest = SchemaManifest::new("0.2.0");
    manifest = manifest
        .with_table(TableSpec::new("provinces"))
        .with_metadata("engine_version", "0.2.0");
    let migrated = MigratedSchemaManifest::new(manifest, "0.1.0");
    assert!(migrated_table_by_name(&migrated, "provinces").is_some());
    assert!(migrated_table_by_name(&migrated, "actors").is_none());
}

// ------------------------------------------------------------------
// Lifecycle tests
// ------------------------------------------------------------------

#[test]
fn load_phase_ordering() {
    assert!(LoadPhase::SnapshotDeserialize < LoadPhase::ModCoordination);
    assert!(LoadPhase::ModCoordination < LoadPhase::SchemaMigration);
    assert!(LoadPhase::SchemaMigration < LoadPhase::ContentCompilation);
    assert!(LoadPhase::ContentCompilation < LoadPhase::SchemaFreeze);
    assert!(LoadPhase::SchemaFreeze < LoadPhase::SimulationStart);
}

#[test]
fn load_phase_next() {
    assert_eq!(
        LoadPhase::SnapshotDeserialize.next(),
        Some(LoadPhase::ModCoordination)
    );
    assert_eq!(LoadPhase::SimulationStart.next(), None);
}

#[test]
fn load_lifecycle_advance_all_phases() {
    let mut lifecycle = LoadLifecycle::new();
    let phases: Vec<LoadPhase> = std::iter::from_fn(|| lifecycle.advance().ok()).collect();
    assert_eq!(phases.len(), 6);
    lifecycle.finish().unwrap();
    assert!(lifecycle.is_finished());
}

#[test]
fn load_lifecycle_cannot_advance_past_end() {
    let mut lifecycle = LoadLifecycle::new();
    for _ in 0..6 {
        lifecycle.advance().unwrap();
    }
    let err = lifecycle.advance().unwrap_err();
    assert!(matches!(err, ContentError::InvalidPhaseTransition { .. }));
}

#[test]
fn load_lifecycle_start_phase_enforces_order() {
    let mut lifecycle = LoadLifecycle::new();
    lifecycle.start_phase(LoadPhase::SchemaMigration).unwrap();
    let err = lifecycle
        .start_phase(LoadPhase::ModCoordination)
        .unwrap_err();
    assert!(matches!(err, ContentError::InvalidPhaseTransition { .. }));
}

#[test]
fn load_lifecycle_manifest_storage() {
    let mut lifecycle = LoadLifecycle::new();
    let manifest = SchemaManifest::new("0.1.0")
        .with_metadata("engine_version", "0.1.0");
    lifecycle.set_manifest(manifest.clone());
    assert_eq!(
        lifecycle.manifest().unwrap().schema_version,
        "0.1.0"
    );
}

#[test]
fn load_lifecycle_migrated_manifest_storage() {
    let mut lifecycle = LoadLifecycle::new();
    let manifest = SchemaManifest::new("0.2.0")
        .with_metadata("engine_version", "0.2.0");
    let migrated = MigratedSchemaManifest::new(manifest, "0.1.0");
    lifecycle.set_migrated_manifest(migrated);
    assert_eq!(
        migrated_target_version(lifecycle.migrated_manifest().unwrap()),
        "0.2.0"
    );
}

#[test]
fn load_lifecycle_frozen_flag() {
    let mut lifecycle = LoadLifecycle::new();
    lifecycle.set_frozen(true);
    assert!(lifecycle.is_frozen());
}

#[test]
fn lifecycle_coordinator_runs_all_phases() {
    let lifecycle = LoadLifecycle::new();
    let mut seen = Vec::new();
    let result = LifecycleCoordinator::run(lifecycle, |_, phase| {
        seen.push(phase);
        Ok(())
    });
    assert!(result.is_ok(), "coordinator failed: {:?}", result);
    assert_eq!(seen.len(), 6);
    assert_eq!(seen.last().copied(), Some(LoadPhase::SimulationStart));
}

#[test]
fn lifecycle_coordinator_error_stops() {
    let lifecycle = LoadLifecycle::new();
    let result = LifecycleCoordinator::run(lifecycle, |_, phase| {
        if phase == LoadPhase::SchemaMigration {
            Err(ContentError::Generic("migration failed".to_owned()))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
}

// ------------------------------------------------------------------
// Compilation tests
// ------------------------------------------------------------------

fn make_actor_spec() -> TableSpec {
    TableSpec::new("actors")
        .with_column(ColumnSpec::new("actor_id", FieldSemantic::Id, "utf8"))
        .unwrap()
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .unwrap()
}

#[test]
fn content_compiler_empty_input() {
    let input = CompilationInput::new();
    let compiler = ContentCompiler::new();
    let output = compiler.compile(input).unwrap();
    assert_eq!(output.store.table_count().unwrap(), 0);
    assert_eq!(output.registry.table_count(), 0);
}

#[test]
fn content_compiler_single_table() {
    let spec = make_actor_spec();
    let mut rows: HashMap<String, Vec<String>> = HashMap::new();
    rows.insert(
        "actor_id".to_owned(),
        vec!["FRA".to_owned(), "ENG".to_owned()],
    );
    rows.insert(
        "name".to_owned(),
        vec!["France".to_owned(), "England".to_owned()],
    );

    let def = RawTableDef { spec, rows };
    let input = CompilationInput::new().with_table(def);
    let compiler = ContentCompiler::new();
    let output = compiler.compile(input).unwrap();

    assert_eq!(output.store.table_count().unwrap(), 1);
    assert_eq!(output.registry.table_count(), 1);
    assert!(output.registry.contains("actors"));
}

#[test]
fn content_compiler_duplicate_table_error() {
    let spec = make_actor_spec();
    let mut rows: HashMap<String, Vec<String>> = HashMap::new();
    rows.insert("actor_id".to_owned(), vec!["FRA".to_owned()]);
    rows.insert("name".to_owned(), vec!["France".to_owned()]);

    let def = RawTableDef {
        spec: spec.clone(),
        rows: rows.clone(),
    };
    let def2 = RawTableDef { spec, rows };
    let input = CompilationInput::new().with_table(def).with_table(def2);
    let compiler = ContentCompiler::new();
    let err = compiler.compile(input).unwrap_err();
    assert!(matches!(err, ContentError::CompilationFailed { .. }));
}

#[test]
fn content_compiler_with_resolver() {
    let mut resolver = NamespaceResolver::new();
    resolver.namespace("actors").register("FRA").unwrap();

    let input = CompilationInput::new().with_resolver(resolver);
    let compiler = ContentCompiler::new();
    let output = compiler.compile(input).unwrap();
    assert!(output.resolver.resolve("actors", "FRA").is_ok());
}

// ------------------------------------------------------------------
// Integration tests
// ------------------------------------------------------------------

#[test]
fn full_load_lifecycle_integration() {
    let mut lifecycle = LoadLifecycle::new();

 // Phase 1: Snapshot Deserialize
    lifecycle.advance().unwrap();
    let manifest = SchemaManifest::new("0.1.0")
        .with_metadata("engine_version", "0.1.0")
        .with_table(TableSpec::new("actors"));
    lifecycle.set_manifest(manifest);

 // Phase 2: Mod Coordination
    lifecycle.advance().unwrap();
    let mods = vec![ModFingerprint::new("base", "1.0")];
    lifecycle.set_final_mods(mods);

 // Phase 3: Schema Migration
    lifecycle.advance().unwrap();
    let migrated = migrated_from_manifest(
        lifecycle.manifest().unwrap(),
        "0.2.0",
    );
    lifecycle.set_migrated_manifest(migrated);

 // Phase 4: Content Compilation
    lifecycle.advance().unwrap();
    let spec = TableSpec::new("actors")
        .with_column(ColumnSpec::new("actor_id", FieldSemantic::Id, "utf8"))
        .unwrap();
    let mut rows: HashMap<String, Vec<String>> = HashMap::new();
    rows.insert("actor_id".to_owned(), vec!["FRA".to_owned()]);
    let def = RawTableDef { spec, rows };
    let input = CompilationInput::new().with_table(def);
    let compiler = ContentCompiler::new();
    let output = compiler.compile(input).unwrap();
    assert_eq!(output.registry.table_count(), 1);

 // Phase 5: Schema Freeze
    lifecycle.advance().unwrap();
    lifecycle.set_frozen(true);
    assert!(lifecycle.is_frozen());

 // Phase 6: Simulation Start
    lifecycle.advance().unwrap();
    lifecycle.finish().unwrap();
    assert!(lifecycle.is_finished());
}

#[test]
fn overlay_plus_name_resolution_integration() {
    let mut resolver = OverlayResolver::new().with_base("actor.FRA.stability", "50");
    let layer = OverlayLayer::new("mod_a", 1).with_entry(OverlayEntry {
        path: "actor.FRA.stability".to_owned(),
        value: "25".to_owned(),
        strategy: MergeStrategy::Add,
    });
    resolver.add_layer(layer);

    let stability = resolver
        .resolve("actor.FRA.stability")
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let mut name_resolver = NameResolver::new();
    let fra_id = name_resolver.register("FRA").unwrap();

    assert_eq!(stability, 75);
    assert_eq!(name_resolver.resolve("FRA").unwrap(), fra_id);
}

#[test]
fn manifest_serialization_includes_fingerprints() {
    let fp = ModFingerprint::new("mod_a", "v1")
        .with_table_spec("actors")
        .with_content_hash("deadbeef");
    let manifest = SchemaManifest::new("0.1.0")
        .with_metadata("engine_version", "0.1.0")
        .with_mod_fingerprint(fp);
    let bytes = manifest_to_bytes(&manifest).unwrap();
    let restored = manifest_from_bytes(&bytes).unwrap();
    assert_eq!(restored.mod_fingerprints.len(), 1);
    assert_eq!(restored.mod_fingerprints[0].mod_id, "mod_a");
}
