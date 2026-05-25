//! Debug-only eframe application entry point.

use super::inspector::InspectorApp;
use crate::panels::{DiffPanel, RelationGraphPanel, SnapshotBrowserPanel, TablePanel};
use scharnhorst_query::QueryEngine;
use std::sync::Arc;

/// Run the inspector GUI in a new window.
/// Returns immediately; the GUI runs on its own thread/event loop.
pub fn run_inspector(
    query_engine: Arc<QueryEngine>,
) -> Result<eframe::Result, Box<dyn std::error::Error>> {
    let mut app = InspectorApp::new(query_engine);

    // Register standard panels
    app.register(Box::new(TablePanel::default()));
    app.register(Box::new(DiffPanel::default()));
    app.register(Box::new(RelationGraphPanel::default()));
    app.register(Box::new(SnapshotBrowserPanel::default()));

    // Spawn on a separate thread to not block the simulation.
    // NativeOptions is not Send, so we construct it inside the closure.
    std::thread::spawn(move || {
        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default()
                .with_inner_size([1200.0, 800.0])
                .with_title("Scharnhorst Inspector"),
            ..Default::default()
        };
        let _ = eframe::run_native(
            "Scharnhorst Inspector",
            options,
            Box::new(|_cc| Ok(Box::new(app))),
        );
    });

    Ok(Ok(()))
}
