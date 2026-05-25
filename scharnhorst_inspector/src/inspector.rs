//! Inspector panel registration and tab infrastructure.

#[cfg(debug_assertions)]
mod inner {
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    /// Trait for inspector panels.
    pub trait InspectorPanel: Send + Sync {
        /// Human-readable name shown in the tab strip.
        fn name(&self) -> &str;

        /// Render the panel content.
        fn ui(&mut self, ui: &mut egui::Ui, ctx: &Context, qe: &Arc<QueryEngine>);
    }

    /// Container for all registered inspector panels.
    pub struct InspectorApp {
        /// Registered panels.
        panels: Vec<Box<dyn InspectorPanel>>,
        /// Index of the currently active panel.
        active_panel: usize,
        /// Shared query engine handle for data access.
        query_engine: Arc<QueryEngine>,
    }

    impl InspectorApp {
        /// Create a new inspector with the given query engine handle.
        pub fn new(query_engine: Arc<QueryEngine>) -> Self {
            Self {
                panels: Vec::new(),
                active_panel: 0,
                query_engine,
            }
        }

        /// Register a panel. Panels are displayed in registration order.
        pub fn register(&mut self, panel: Box<dyn InspectorPanel>) {
            self.panels.push(panel);
        }

        /// Get the number of registered panels.
        pub fn panel_count(&self) -> usize {
            self.panels.len()
        }
    }

    impl eframe::App for InspectorApp {
        fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
            egui::TopBottomPanel::top("tab_strip").show(ctx, |ui| {
                ui.horizontal(|ui| {
                    for (i, panel) in self.panels.iter().enumerate() {
                        let selected = i == self.active_panel;
                        if ui.selectable_label(selected, panel.name()).clicked() {
                            self.active_panel = i;
                        }
                    }
                });
            });

            egui::CentralPanel::default().show(ctx, |ui| {
                if let Some(panel) = self.panels.get_mut(self.active_panel) {
                    panel.ui(ui, ctx, &self.query_engine);
                } else {
                    ui.label("No panels registered.");
                }
            });
        }
    }
}

#[cfg(not(debug_assertions))]
mod inner {
    // In release, InspectorPanel is a no-op trait for API compatibility.
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    /// Release-mode no-op trait placeholder.
    pub trait InspectorPanel {
        fn name(&self) -> &str;
        fn ui(&mut self, ui: &mut egui::Ui, ctx: &Context, qe: &Arc<QueryEngine>);
    }

    pub struct InspectorApp;

    impl InspectorApp {
        pub fn new(_qe: Arc<QueryEngine>) -> Self {
            Self
        }
        pub fn panel_count(&self) -> usize {
            0
        }
    }
}

pub use inner::*;
