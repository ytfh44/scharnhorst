//! Snapshot browser panel: timeline scrubber to inspect historical ticks.

#[cfg(debug_assertions)]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::{Align, Context, Layout};
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    pub struct SnapshotBrowserPanel {
        /// Current tick being viewed (may differ from simulation's current tick).
        viewing_tick: u64,
        /// Simulation's current tick (updated each frame).
        current_sim_tick: u64,
        /// Maximum tick that has existed (auto-expanding).
        max_known_tick: u64,
        /// Selected table for detail view.
        selected_table: Option<String>,
        /// Table names for dropdown.
        table_names: Vec<String>,
        /// Columns of the selected table.
        columns: Vec<String>,
        /// Rows of the selected table at the viewing tick.
        rows: Vec<Vec<String>>,
        /// Pending load trigger.
        needs_load: bool,
        /// Max rows to display.
        max_rows: usize,
    }

    impl Default for SnapshotBrowserPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SnapshotBrowserPanel {
        pub fn new() -> Self {
            Self {
                viewing_tick: 0,
                current_sim_tick: 0,
                max_known_tick: 0,
                selected_table: None,
                table_names: Vec::new(),
                columns: Vec::new(),
                rows: Vec::new(),
                needs_load: true,
                max_rows: 200,
            }
        }

        pub fn set_viewing_tick(&mut self, tick: u64) {
            self.viewing_tick = tick;
            self.needs_load = true;
        }

        pub fn viewing_tick(&self) -> u64 {
            self.viewing_tick
        }

        pub fn max_known_tick(&self) -> u64 {
            self.max_known_tick
        }

        pub fn set_selected_table(&mut self, name: Option<String>) {
            self.selected_table = name;
            self.needs_load = true;
        }

        pub fn selected_table_name(&self) -> Option<&str> {
            self.selected_table.as_deref()
        }

        pub fn columns(&self) -> &[String] {
            &self.columns
        }

        pub fn rows(&self) -> &[Vec<String>] {
            &self.rows
        }

        pub fn load_tick_data(&mut self, qe: &Arc<QueryEngine>) {
            self.columns.clear();
            self.rows.clear();

            let Ok(snapshot) = qe.snapshot() else {
                return;
            };
            self.current_sim_tick = snapshot.tick().as_u64();
            if self.current_sim_tick > self.max_known_tick {
                self.max_known_tick = self.current_sim_tick;
            }

            // Clamp viewing tick to valid range
            if self.viewing_tick > self.max_known_tick {
                self.viewing_tick = self.max_known_tick;
            }
            if self.viewing_tick == 0 {
                self.viewing_tick = self.max_known_tick;
            }

            self.table_names = snapshot.table_names();

            // Load selected table data
            if let Some(ref table) = self.selected_table {
                if let Ok(cols) = snapshot.column_names(table) {
                    self.columns = cols;
                }
                if let Ok(mut iter) = snapshot.iter_rows(table) {
                    let mut count: usize = 0;
                    while count < self.max_rows {
                        match iter.next() {
                            Some(Ok(row)) => {
                                let mut str_row: Vec<String> = Vec::new();
                                for i in 0..self.columns.len() {
                                    let val = if let Ok(v) = row.get_i64(i) {
                                        v.map(|x| x.to_string())
                                            .unwrap_or_else(|| "NULL".to_owned())
                                    } else if let Ok(v) = row.get_f64(i) {
                                        v.map(|x| x.to_string())
                                            .unwrap_or_else(|| "NULL".to_owned())
                                    } else if let Ok(v) = row.get_string(i) {
                                        v.unwrap_or_else(|| "NULL".to_owned())
                                    } else if let Ok(v) = row.get_bool(i) {
                                        v.map(|x| x.to_string())
                                            .unwrap_or_else(|| "NULL".to_owned())
                                    } else {
                                        "?".to_owned()
                                    };
                                    str_row.push(val);
                                }
                                self.rows.push(str_row);
                                count += 1;
                            }
                            Some(Err(_)) => break,
                            None => break,
                        }
                    }
                }
            }

            self.needs_load = false;
        }
    }

    impl InspectorPanel for SnapshotBrowserPanel {
        fn name(&self) -> &str {
            "Snapshot Browser"
        }

        fn ui(&mut self, ui: &mut egui::Ui, _ctx: &Context, qe: &Arc<QueryEngine>) {
            // Tick slider
            ui.horizontal(|ui| {
                ui.label("Tick:");
                let slider_range = 0..=self.max_known_tick;
                let mut tick_val = self.viewing_tick;
                if ui
                    .add(egui::Slider::new(&mut tick_val, slider_range).text("tick"))
                    .changed()
                {
                    self.viewing_tick = tick_val;
                }

                // Buttons for fine navigation
                if ui.button("\u{25C0}").clicked() && self.viewing_tick > 0 {
                    self.viewing_tick = self.viewing_tick.saturating_sub(1);
                }
                if ui.button("\u{25B6}").clicked() && self.viewing_tick < self.max_known_tick {
                    self.viewing_tick = self.viewing_tick.saturating_add(1);
                }

                ui.label(format!("Sim tick: {}", self.current_sim_tick));

                if ui.button("Refresh").clicked() {
                    self.needs_load = true;
                }
            });

            ui.separator();

            if self.needs_load {
                self.load_tick_data(qe);
            }

            // Table selector
            ui.horizontal(|ui| {
                ui.label("Table:");
                let mut current = self.selected_table.clone().unwrap_or_default();
                let response = egui::ComboBox::from_id_salt("sb_table_selector")
                    .selected_text(&current)
                    .show_ui(ui, |ui| {
                        for name in &self.table_names {
                            if ui
                                .selectable_value(&mut current, name.clone(), name)
                                .changed()
                            {
                                self.selected_table = Some(name.clone());
                                self.needs_load = true;
                            }
                        }
                    })
                    .response;
                let _ = response;
            });

            ui.separator();

            // Data grid (same pattern as TablePanel)
            if self.selected_table.is_some() {
                egui::ScrollArea::both().show(ui, |ui| {
                    egui::Grid::new("sb_table_grid")
                        .striped(true)
                        .show(ui, |ui| {
                            for col in &self.columns {
                                ui.label(egui::RichText::new(col).strong());
                            }
                            ui.end_row();
                            for row in &self.rows {
                                for cell in row {
                                    ui.label(cell);
                                }
                                ui.end_row();
                            }
                        });
                });
            } else {
                ui.label("Select a table to view snapshot data.");
                ui.label("Note: snapshot browser shows current tick data; historical snapshots require journal replay support.");
            }

            // Info footer
            ui.with_layout(Layout::bottom_up(Align::RIGHT), |ui| {
                ui.label(format!(
                    "Viewing tick {} / sim tick {}",
                    self.viewing_tick, self.current_sim_tick
                ));
            });
        }
    }
}

#[cfg(not(debug_assertions))]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    pub struct SnapshotBrowserPanel;

    impl Default for SnapshotBrowserPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SnapshotBrowserPanel {
        pub fn new() -> Self {
            Self
        }
    }

    impl InspectorPanel for SnapshotBrowserPanel {
        fn name(&self) -> &str {
            "Snapshot Browser"
        }
        fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &Context, _qe: &Arc<QueryEngine>) {}
    }
}

pub use inner::SnapshotBrowserPanel;
