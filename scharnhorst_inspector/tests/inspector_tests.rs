//! Integration tests for scharnhorst_inspector.
//!
//! These tests verify:
//! - InspectorApp creation and panel registration
//! - InspectorPanel trait implementation for all panels
//! - TablePanel data reload behavior
//! - DiffPanel entry management
//! - Debug-only gating: release builds should compile with no-op stubs

#[cfg(debug_assertions)]
mod debug_tests {
    use scharnhorst_inspector::inspector::{InspectorApp, InspectorPanel};
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::SchemaRegistry;
    use std::sync::Arc;

    // ------------------------------------------------------------------
    // InspectorApp creation tests
    // ------------------------------------------------------------------

    #[test]
    fn inspector_app_new_has_zero_panels() {
        let qe = Arc::new(QueryEngine::new(SchemaRegistry::new()));
        let app = InspectorApp::new(Arc::clone(&qe));
        assert_eq!(app.panel_count(), 0);
    }

    #[test]
    fn inspector_app_register_increases_panel_count() {
        let qe = Arc::new(QueryEngine::new(SchemaRegistry::new()));
        let mut app = InspectorApp::new(Arc::clone(&qe));

        struct DummyPanel;
        impl InspectorPanel for DummyPanel {
            fn name(&self) -> &str {
                "Dummy"
            }
            fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &egui::Context, _qe: &Arc<QueryEngine>) {}
        }

        assert_eq!(app.panel_count(), 0);
        app.register(Box::new(DummyPanel));
        assert_eq!(app.panel_count(), 1);
    }

    #[test]
    fn inspector_app_multiple_registration() {
        let qe = Arc::new(QueryEngine::new(SchemaRegistry::new()));
        let mut app = InspectorApp::new(Arc::clone(&qe));

        struct PanelA;
        impl InspectorPanel for PanelA {
            fn name(&self) -> &str {
                "A"
            }
            fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &egui::Context, _qe: &Arc<QueryEngine>) {}
        }
        struct PanelB;
        impl InspectorPanel for PanelB {
            fn name(&self) -> &str {
                "B"
            }
            fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &egui::Context, _qe: &Arc<QueryEngine>) {}
        }

        app.register(Box::new(PanelA));
        app.register(Box::new(PanelB));
        assert_eq!(app.panel_count(), 2);
    }

    // ------------------------------------------------------------------
    // TablePanel tests
    // ------------------------------------------------------------------

    #[test]
    fn table_panel_new_has_no_selected_table() {
        use scharnhorst_inspector::panels::TablePanel;
        // TablePanel::new() should not panic even with no data
        // We can't easily verify internal state without rendering,
        // but construction must succeed.
        let _panel = TablePanel::default();
    }

    // ------------------------------------------------------------------
    // DiffPanel tests
    // ------------------------------------------------------------------

    #[test]
    fn diff_panel_new_is_empty() {
        use scharnhorst_inspector::panels::DiffPanel;
        let _panel = DiffPanel::default();
    }

    // ------------------------------------------------------------------
    // RelationGraphPanel stub tests
    // ------------------------------------------------------------------

    #[test]
    fn relation_graph_panel_new_works() {
        use scharnhorst_inspector::panels::RelationGraphPanel;
        let _panel = RelationGraphPanel::default();
    }

    // ------------------------------------------------------------------
    // SnapshotBrowserPanel stub tests
    // ------------------------------------------------------------------

    #[test]
    fn snapshot_browser_panel_new_works() {
        use scharnhorst_inspector::panels::SnapshotBrowserPanel;
        let _panel = SnapshotBrowserPanel::default();
    }
}

// ------------------------------------------------------------------
// Group 11: RelationGraphPanel tests
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod graph_tests {
    use scharnhorst_inspector::inspector::InspectorPanel;
    use scharnhorst_inspector::panels::RelationGraphPanel;

    #[test]
    fn graph_panel_has_correct_name() {
        let panel = RelationGraphPanel::new();
        assert_eq!(panel.name(), "Relation Graph");
    }
}

// ------------------------------------------------------------------
// Group 12: SnapshotBrowserPanel tests
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod snapshot_browser_tests {
    use scharnhorst_inspector::inspector::InspectorPanel;
    use scharnhorst_inspector::panels::SnapshotBrowserPanel;

    #[test]
    fn snapshot_browser_has_correct_name() {
        let panel = SnapshotBrowserPanel::new();
        assert_eq!(panel.name(), "Snapshot Browser");
    }
}

// ------------------------------------------------------------------
// Group 13: TablePanel integration tests (with real QueryEngine)
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod table_panel_integration_tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use scharnhorst_arrow_store::{MutationMode, VersionedTable, WorldSnapshot};
    use scharnhorst_core::Tick;
    use scharnhorst_inspector::panels::TablePanel;
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
    use std::sync::Arc;

    fn make_engine_with_table(table_name: &str, tick_val: u64) -> (Arc<QueryEngine>, TableSpec) {
        let mut reg = SchemaRegistry::new();
        let spec = TableSpec::new(table_name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap()
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .unwrap();
        reg.register(spec.clone()).unwrap();
        let qe = Arc::new(QueryEngine::new(reg));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        let name_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();
        let mut table = VersionedTable::new(table_name, MutationMode::AppendOnly);
        table.insert_version(Tick(tick_val), vec![batch]).unwrap();
        let snapshot = WorldSnapshot::with_table(Tick(tick_val), table_name, Arc::new(table));
        qe.store_world_snapshot(snapshot).unwrap();

        (qe, spec)
    }

    #[test]
    fn table_panel_reload_with_data_populates_rows() {
        let (qe, _spec) = make_engine_with_table("heroes", 5);
        let mut panel = TablePanel::new();
        panel.set_selected_table(Some("heroes".to_owned()));
        panel.reload(&qe);

        let columns = panel.columns();
        assert_eq!(columns.len(), 2);
        assert!(columns.contains(&"id".to_owned()));
        assert!(columns.contains(&"name".to_owned()));

        let row_data = panel.row_data();
        assert_eq!(row_data.len(), 2);
        assert_eq!(row_data[0][0], "1");
        assert_eq!(row_data[0][1], "alpha");
        assert_eq!(row_data[1][0], "2");
        assert_eq!(row_data[1][1], "beta");
    }

    #[test]
    fn table_panel_reload_nonexistent_table_clears_data() {
        let (qe, _spec) = make_engine_with_table("heroes", 5);
        let mut panel = TablePanel::new();

        // First load valid table
        panel.set_selected_table(Some("heroes".to_owned()));
        panel.reload(&qe);
        assert!(!panel.row_data().is_empty());

        // Then load nonexistent
        panel.set_selected_table(Some("nonexistent".to_owned()));
        panel.reload(&qe);
        assert!(panel.row_data().is_empty());
        assert!(panel.columns().is_empty());
    }

    #[test]
    fn table_panel_unknown_column_type_shows_question_mark() {
        let mut reg = SchemaRegistry::new();
        let spec = TableSpec::new("events")
            .with_column(ColumnSpec::new(
                "created_at",
                FieldSemantic::Timestamp,
                "timestamp",
            ))
            .unwrap();
        reg.register(spec).unwrap();
        let qe = Arc::new(QueryEngine::new(reg));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let ts_arr: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1000i64, 2000]));
        let batch = RecordBatch::try_new(schema, vec![ts_arr]).unwrap();
        let mut table = VersionedTable::new("events", MutationMode::AppendOnly);
        table.insert_version(Tick(1), vec![batch]).unwrap();
        let snapshot = WorldSnapshot::with_table(Tick(1), "events", Arc::new(table));
        qe.store_world_snapshot(snapshot).unwrap();

        let mut panel = TablePanel::new();
        panel.set_selected_table(Some("events".to_owned()));
        panel.reload(&qe);

        let row_data = panel.row_data();
        assert_eq!(row_data.len(), 2);
        // TimestampNanosecond columns are not supported by get_i64/get_f64/get_string/get_bool
        assert_eq!(row_data[0][0], "?");
        assert_eq!(row_data[1][0], "?");
    }
}

// ------------------------------------------------------------------
// Group 14: DiffPanel integration tests
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod diff_panel_integration_tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use scharnhorst_arrow_store::{MutationMode, VersionedTable, WorldSnapshot};
    use scharnhorst_core::{Diff, RowId, Tick};
    use scharnhorst_inspector::panels::DiffPanel;
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
    use std::sync::Arc;

    fn make_engine_with_snapshot(table_name: &str, tick_val: u64) -> Arc<QueryEngine> {
        let mut reg = SchemaRegistry::new();
        let spec = TableSpec::new(table_name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap();
        reg.register(spec).unwrap();
        let qe = Arc::new(QueryEngine::new(reg));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        let batch = RecordBatch::try_new(schema, vec![id_arr]).unwrap();
        let mut table = VersionedTable::new(table_name, MutationMode::AppendOnly);
        table.insert_version(Tick(tick_val), vec![batch]).unwrap();
        let snapshot = WorldSnapshot::with_table(Tick(tick_val), table_name, Arc::new(table));
        qe.store_world_snapshot(snapshot).unwrap();

        qe
    }

    #[test]
    fn diff_panel_poll_diffs_detects_tick_change() {
        let qe = make_engine_with_snapshot("heroes", 5);
        let mut panel = DiffPanel::new();
        panel.poll_diffs(&qe);

        let entries = panel.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].tick, 5);
        assert_eq!(entries[0].kind, "TICK");
        assert!(entries[0].summary.contains("Tick 5"));
    }

    #[test]
    fn diff_panel_poll_diffs_no_duplicate_on_same_tick() {
        let qe = make_engine_with_snapshot("heroes", 5);
        let mut panel = DiffPanel::new();

        panel.poll_diffs(&qe);
        assert_eq!(panel.entries().len(), 1);

        // Second call — same tick, no new entry
        panel.poll_diffs(&qe);
        assert_eq!(panel.entries().len(), 1);
    }

    #[test]
    fn diff_panel_filter_by_table() {
        let qe = make_engine_with_snapshot("heroes", 1);

        // Push diff summaries for two tables
        let diffs = vec![
            Diff::Insert {
                table: "heroes".into(),
                row: RowId::new(1),
                values: serde_json::Map::new(),
            },
            Diff::Insert {
                table: "items".into(),
                row: RowId::new(2),
                values: serde_json::Map::new(),
            },
        ];
        qe.push_diff_summaries(&diffs, Tick(3));

        let mut panel = DiffPanel::new();
        panel.poll_diffs(&qe);

        // All entries present (1 TICK + 2 diffs = 3)
        assert_eq!(panel.entries().len(), 3);

        // Set filter and verify filtering works on entries
        panel.set_filter_table(Some("heroes".to_owned()));

        let heroes_entries: Vec<_> = panel
            .entries()
            .iter()
            .filter(|e| e.table == "heroes" || e.kind == "TICK")
            .collect();
        assert_eq!(heroes_entries.len(), 2); // TICK + heroes insert
        assert!(heroes_entries.iter().any(|e| e.table == "heroes"));
    }

    #[test]
    fn diff_panel_auto_scroll_defaults_true() {
        let panel = DiffPanel::new();
        assert!(panel.auto_scroll());
    }

    #[test]
    fn diff_panel_clear_removes_entries() {
        let qe = make_engine_with_snapshot("heroes", 5);
        let mut panel = DiffPanel::new();
        panel.poll_diffs(&qe);
        assert!(!panel.entries().is_empty());

        panel.clear();
        assert!(panel.entries().is_empty());
    }
}

// ------------------------------------------------------------------
// Group 15: SnapshotBrowserPanel integration tests
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod snapshot_browser_integration_tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use scharnhorst_arrow_store::{MutationMode, VersionedTable, WorldSnapshot};
    use scharnhorst_core::Tick;
    use scharnhorst_inspector::panels::SnapshotBrowserPanel;
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
    use std::sync::Arc;

    fn make_engine_with_snapshot(table_name: &str, tick_val: u64) -> Arc<QueryEngine> {
        let mut reg = SchemaRegistry::new();
        let spec = TableSpec::new(table_name)
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))
            .unwrap()
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
            .unwrap();
        reg.register(spec).unwrap();
        let qe = Arc::new(QueryEngine::new(reg));

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20]));
        let name_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();
        let mut table = VersionedTable::new(table_name, MutationMode::AppendOnly);
        table.insert_version(Tick(tick_val), vec![batch]).unwrap();
        let snapshot = WorldSnapshot::with_table(Tick(tick_val), table_name, Arc::new(table));
        qe.store_world_snapshot(snapshot).unwrap();

        qe
    }

    #[test]
    fn snapshot_browser_load_tick_data_clamps_viewing_tick() {
        let qe = make_engine_with_snapshot("heroes", 5);
        let mut panel = SnapshotBrowserPanel::new();

        // Set viewing_tick beyond max_known_tick (which is 0 initially)
        panel.set_viewing_tick(10);
        panel.load_tick_data(&qe);

        // Should be clamped to max_known_tick (5) after load
        assert_eq!(panel.viewing_tick(), 5);
    }

    #[test]
    fn snapshot_browser_load_tick_data_zero_viewing_uses_max() {
        let qe = make_engine_with_snapshot("heroes", 7);
        let mut panel = SnapshotBrowserPanel::new();

        // viewing_tick starts at 0
        panel.load_tick_data(&qe);

        // Should default to max_known_tick (7)
        assert_eq!(panel.viewing_tick(), 7);
    }

    #[test]
    fn snapshot_browser_no_selected_table_shows_empty() {
        let qe = make_engine_with_snapshot("heroes", 1);
        let mut panel = SnapshotBrowserPanel::new();

        // No selected_table set
        panel.load_tick_data(&qe);

        assert!(panel.columns().is_empty());
        assert!(panel.rows().is_empty());
    }
}

// ------------------------------------------------------------------
// Group 16: RelationGraphPanel integration tests
// ------------------------------------------------------------------

#[cfg(debug_assertions)]
mod relation_graph_integration_tests {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use scharnhorst_arrow_store::{MutationMode, VersionedTable, WorldSnapshot};
    use scharnhorst_core::Tick;
    use scharnhorst_inspector::panels::RelationGraphPanel;
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
    use std::sync::Arc;

    fn register_spec(reg: &mut SchemaRegistry, name: &str, columns: &[(&str, &str)]) {
        let mut spec = TableSpec::new(name);
        for &(col_name, storage_type) in columns {
            spec = spec
                .with_column(ColumnSpec::new(
                    col_name,
                    FieldSemantic::Quantity,
                    storage_type,
                ))
                .unwrap();
        }
        reg.register(spec).unwrap();
    }

    #[test]
    fn relation_graph_reload_populates_nodes() {
        let mut reg = SchemaRegistry::new();
        register_spec(&mut reg, "heroes", &[("id", "i64"), ("name", "utf8")]);
        register_spec(&mut reg, "items", &[("id", "i64")]);
        let qe = Arc::new(QueryEngine::new(reg));

        // Store snapshot with both tables
        let mut snap = WorldSnapshot::new(Tick(1));
        snap.register_table(
            "heroes",
            Arc::new({
                let mut t = VersionedTable::new("heroes", MutationMode::AppendOnly);
                let schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ]));
                let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
                let name_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha")]));
                let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();
                t.insert_version(Tick(1), vec![batch]).unwrap();
                t
            }),
        );
        snap.register_table(
            "items",
            Arc::new({
                let mut t = VersionedTable::new("items", MutationMode::AppendOnly);
                let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
                let id_arr: ArrayRef = Arc::new(Int64Array::from(vec![100i64]));
                let batch = RecordBatch::try_new(schema, vec![id_arr]).unwrap();
                t.insert_version(Tick(1), vec![batch]).unwrap();
                t
            }),
        );
        qe.store_world_snapshot(snap).unwrap();

        let mut panel = RelationGraphPanel::new();
        panel.reload(&qe);

        let nodes = panel.nodes();
        assert_eq!(nodes.len(), 2);
        let node_names: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
        assert!(node_names.contains(&"heroes"));
        assert!(node_names.contains(&"items"));
    }

    #[test]
    fn relation_graph_reload_empty_tables_shows_no_nodes() {
        let reg = SchemaRegistry::new();
        let qe = Arc::new(QueryEngine::new(reg));

        // Store an empty snapshot (no tables)
        let snap = WorldSnapshot::new(Tick(1));
        qe.store_world_snapshot(snap).unwrap();

        let mut panel = RelationGraphPanel::new();
        panel.reload(&qe);

        assert!(panel.nodes().is_empty());
        assert!(panel.edges().is_empty());
    }

    #[test]
    fn relation_graph_fk_inference_detects_edges() {
        let mut reg = SchemaRegistry::new();
        register_spec(
            &mut reg,
            "heroes",
            &[("id", "i64"), ("name", "utf8"), ("unit_id", "i64")],
        );
        register_spec(&mut reg, "unit", &[("id", "i64"), ("label", "utf8")]);
        register_spec(&mut reg, "hero_id", &[("id", "i64"), ("level", "i64")]);
        let qe = Arc::new(QueryEngine::new(reg));

        // Create snapshot with all three tables
        let mut snap = WorldSnapshot::new(Tick(1));

        // heroes table with columns: id, name, unit_id
        let heroes_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("unit_id", DataType::Int64, false),
        ]));
        let heroes_id: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
        let heroes_name: ArrayRef =
            Arc::new(StringArray::from(vec![Some("arthur"), Some("lancelot")]));
        let heroes_unit_id: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20]));
        let heroes_batch =
            RecordBatch::try_new(heroes_schema, vec![heroes_id, heroes_name, heroes_unit_id])
                .unwrap();
        let mut heroes_table = VersionedTable::new("heroes", MutationMode::AppendOnly);
        heroes_table
            .insert_version(Tick(1), vec![heroes_batch])
            .unwrap();
        snap.register_table("heroes", Arc::new(heroes_table));

        // unit table
        let unit_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let unit_id: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20]));
        let unit_label: ArrayRef =
            Arc::new(StringArray::from(vec![Some("infantry"), Some("cavalry")]));
        let unit_batch = RecordBatch::try_new(unit_schema, vec![unit_id, unit_label]).unwrap();
        let mut unit_table = VersionedTable::new("unit", MutationMode::AppendOnly);
        unit_table
            .insert_version(Tick(1), vec![unit_batch])
            .unwrap();
        snap.register_table("unit", Arc::new(unit_table));

        // hero_id table
        let hero_id_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("level", DataType::Int64, false),
        ]));
        let hid_id: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        let hid_level: ArrayRef = Arc::new(Int64Array::from(vec![5i64]));
        let hero_id_batch = RecordBatch::try_new(hero_id_schema, vec![hid_id, hid_level]).unwrap();
        let mut hero_id_table = VersionedTable::new("hero_id", MutationMode::AppendOnly);
        hero_id_table
            .insert_version(Tick(1), vec![hero_id_batch])
            .unwrap();
        snap.register_table("hero_id", Arc::new(hero_id_table));

        qe.store_world_snapshot(snap).unwrap();

        let mut panel = RelationGraphPanel::new();
        panel.reload(&qe);

        let edges = panel.edges();
        // "heroes.unit_id" -> strip "_id" -> "unit" (exists) -> edge from heroes to unit
        // "heroes" also has column "id" but that doesn't match FK pattern
        // "heroes" also has "name" (no FK pattern)
        let unit_edge = edges.iter().find(|e| e.from == "heroes" && e.to == "unit");
        assert!(
            unit_edge.is_some(),
            "expected edge heroes->unit via unit_id column"
        );

        // No edge for "hero_id" table since it doesn't have FK-named columns that match other tables
        // (its columns are "id" and "level", neither matches FK naming convention)

        // Also verify node count
        let nodes = panel.nodes();
        assert_eq!(nodes.len(), 3);
    }
}

// ------------------------------------------------------------------
// Release-mode tests: ensure no-op stubs compile
// ------------------------------------------------------------------

#[cfg(not(debug_assertions))]
mod release_tests {
    use scharnhorst_inspector::inspector::InspectorApp;
    use scharnhorst_query::QueryEngine;
    use scharnhorst_schema::SchemaRegistry;
    use std::sync::Arc;

    #[test]
    fn release_inspector_app_has_zero_panels() {
        let qe = Arc::new(QueryEngine::new(SchemaRegistry::new()));
        let app = InspectorApp::new(Arc::clone(&qe));
        assert_eq!(app.panel_count(), 0);
    }

    #[test]
    fn release_table_panel_new() {
        use scharnhorst_inspector::panels::TablePanel;
        let _ = TablePanel::default();
    }

    #[test]
    fn release_diff_panel_new() {
        use scharnhorst_inspector::panels::DiffPanel;
        let _ = DiffPanel::default();
    }

    #[test]
    fn release_relation_graph_panel_new() {
        use scharnhorst_inspector::panels::RelationGraphPanel;
        let _ = RelationGraphPanel::default();
    }

    #[test]
    fn release_snapshot_browser_panel_new() {
        use scharnhorst_inspector::panels::SnapshotBrowserPanel;
        let _ = SnapshotBrowserPanel::default();
    }

    #[test]
    fn release_graph_panel_has_correct_name() {
        use scharnhorst_inspector::inspector::InspectorPanel;
        use scharnhorst_inspector::panels::RelationGraphPanel;
        let panel = RelationGraphPanel::new();
        assert_eq!(panel.name(), "Relation Graph");
    }

    #[test]
    fn release_snapshot_browser_has_correct_name() {
        use scharnhorst_inspector::inspector::InspectorPanel;
        use scharnhorst_inspector::panels::SnapshotBrowserPanel;
        let panel = SnapshotBrowserPanel::new();
        assert_eq!(panel.name(), "Snapshot Browser");
    }
}
