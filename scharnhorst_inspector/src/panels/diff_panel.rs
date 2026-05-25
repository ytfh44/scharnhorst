//! Diff stream panel: live tail log of committed diffs.

#[cfg(debug_assertions)]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    /// Ring buffer capacity for diff entries.
    const MAX_ENTRIES: usize = 500;

    pub struct DiffPanel {
        /// Ring buffer of diff entries.
        entries: Vec<DiffEntry>,
        /// Filter: only show diffs for this table (None = show all).
        filter_table: Option<String>,
        /// Whether to auto-scroll to bottom.
        auto_scroll: bool,
        /// Known table names for the filter dropdown.
        table_names: Vec<String>,
        /// Last tick seen (for table_names refresh).
        last_tick: u64,
        /// Number of diff summaries already consumed.
        seen_summary_count: usize,
    }

    pub struct DiffEntry {
        pub tick: u64,
        pub table: String,
        pub kind: String,
        pub summary: String,
    }

    impl Default for DiffPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl DiffPanel {
        pub fn new() -> Self {
            Self {
                entries: Vec::new(),
                filter_table: None,
                auto_scroll: true,
                table_names: Vec::new(),
                last_tick: 0,
                seen_summary_count: 0,
            }
        }

        pub fn set_filter_table(&mut self, filter: Option<String>) {
            self.filter_table = filter;
        }

        pub fn auto_scroll(&self) -> bool {
            self.auto_scroll
        }

        pub fn entries(&self) -> &[DiffEntry] {
            &self.entries
        }

        pub fn last_tick(&self) -> u64 {
            self.last_tick
        }

        pub fn clear(&mut self) {
            self.entries.clear();
            self.seen_summary_count = 0;
        }

        pub fn poll_diffs(&mut self, qe: &Arc<QueryEngine>) {
            let Ok(snapshot) = qe.snapshot() else {
                return;
            };
            let current_tick = snapshot.tick().as_u64();

            // Refresh table names on tick change
            if current_tick > self.last_tick {
                self.last_tick = current_tick;
                self.table_names = snapshot.table_names();

                self.entries.push(DiffEntry {
                    tick: current_tick,
                    table: String::new(),
                    kind: "TICK".to_owned(),
                    summary: format!("Tick {} boundary", current_tick),
                });
            }

            // Pull new diff summaries from the query engine ring buffer
            let summaries = qe.diff_summaries();
            if summaries.len() > self.seen_summary_count {
                for summary in &summaries[self.seen_summary_count..] {
                    self.entries.push(DiffEntry {
                        tick: summary.tick,
                        table: summary.table_name.clone(),
                        kind: summary.diff_kind.clone(),
                        summary: summary.summary.clone(),
                    });
                }
                self.seen_summary_count = summaries.len();
            }

            // Trim entries to capacity
            while self.entries.len() > MAX_ENTRIES {
                self.entries.remove(0);
            }
        }
    }

    impl InspectorPanel for DiffPanel {
        fn name(&self) -> &str {
            "Diff Stream"
        }

        fn ui(&mut self, ui: &mut egui::Ui, _ctx: &Context, qe: &Arc<QueryEngine>) {
            // Poll for new diffs
            self.poll_diffs(qe);

            // Controls
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.auto_scroll, "Auto-scroll");

                ui.label("Filter table:");
                let mut current = self
                    .filter_table
                    .clone()
                    .unwrap_or_else(|| "All".to_owned());
                let response = egui::ComboBox::from_id_salt("diff_filter")
                    .selected_text(&current)
                    .show_ui(ui, |ui| {
                        if ui
                            .selectable_value(&mut current, "All".to_owned(), "All")
                            .changed()
                        {
                            self.filter_table = None;
                        }
                        for name in &self.table_names {
                            if ui
                                .selectable_value(&mut current, name.clone(), name)
                                .changed()
                            {
                                self.filter_table = Some(name.clone());
                            }
                        }
                    })
                    .response;
                let _ = response;

                if ui.button("Clear").clicked() {
                    self.entries.clear();
                    self.seen_summary_count = 0;
                }
            });

            ui.separator();

            // Diff log
            let filtered: Vec<&DiffEntry> = if let Some(ref filter) = self.filter_table {
                self.entries
                    .iter()
                    .filter(|e| e.table == *filter || e.kind == "TICK")
                    .collect()
            } else {
                self.entries.iter().collect()
            };

            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .show(ui, |ui| {
                    for entry in &filtered {
                        ui.horizontal(|ui| {
                            ui.label(format!("[t{}]", entry.tick));
                            if !entry.table.is_empty() {
                                ui.label(entry.table.as_str());
                            }
                            ui.colored_label(diff_color(&entry.kind), entry.kind.as_str());
                            ui.label(entry.summary.as_str());
                        });
                    }
                });
        }
    }

    fn diff_color(kind: &str) -> egui::Color32 {
        match kind {
            "Insert" => egui::Color32::from_rgb(100, 200, 100),
            "Update" => egui::Color32::from_rgb(200, 200, 80),
            "Delete" => egui::Color32::from_rgb(220, 80, 80),
            "ReplaceTable" => egui::Color32::from_rgb(80, 160, 220),
            _ => egui::Color32::GRAY,
        }
    }
}

#[cfg(not(debug_assertions))]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    pub struct DiffPanel;

    impl Default for DiffPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl DiffPanel {
        pub fn new() -> Self {
            Self
        }
    }

    impl InspectorPanel for DiffPanel {
        fn name(&self) -> &str {
            "Diff Stream"
        }
        fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &Context, _qe: &Arc<QueryEngine>) {}
    }
}

pub use inner::DiffPanel;
