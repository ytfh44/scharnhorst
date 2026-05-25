//! Table viewer panel: dropdown to pick table, then row browser.

#[cfg(debug_assertions)]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    pub struct TablePanel {
        /// Currently selected table name (None = none selected).
        selected_table: Option<String>,
        /// Table names for the dropdown (refreshed each frame).
        table_names: Vec<String>,
        /// Columns of the selected table.
        columns: Vec<String>,
        /// Rendered rows data (WorldView rows cached).
        row_data: Vec<Vec<String>>,
        /// Number of rows to display (max, to avoid performance issues).
        max_rows: usize,
        /// Pending reload flag.
        needs_reload: bool,
    }

    impl Default for TablePanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TablePanel {
        pub fn new() -> Self {
            Self {
                selected_table: None,
                table_names: Vec::new(),
                columns: Vec::new(),
                row_data: Vec::new(),
                max_rows: 200,
                needs_reload: true,
            }
        }

        pub fn set_selected_table(&mut self, name: Option<String>) {
            self.selected_table = name;
            self.needs_reload = true;
        }

        pub fn selected_table_name(&self) -> Option<&str> {
            self.selected_table.as_deref()
        }

        pub fn columns(&self) -> &[String] {
            &self.columns
        }

        pub fn row_data(&self) -> &[Vec<String>] {
            &self.row_data
        }

        pub fn reload(&mut self, qe: &Arc<QueryEngine>) {
            self.columns.clear();
            self.row_data.clear();

            let Some(ref table) = self.selected_table else {
                return;
            };

            let Ok(snapshot) = qe.snapshot() else {
                return;
            };

            if let Ok(cols) = snapshot.column_names(table) {
                self.columns = cols;
            }

            if let Ok(mut iter) = snapshot.iter_rows(table) {
                let mut count = 0;
                while count < self.max_rows {
                    match iter.next() {
                        Some(Ok(row)) => {
                            let mut str_row = Vec::new();
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
                            self.row_data.push(str_row);
                            count += 1;
                        }
                        Some(Err(_)) => break,
                        None => break,
                    }
                }
            }

            self.needs_reload = false;
        }
    }

    impl InspectorPanel for TablePanel {
        fn name(&self) -> &str {
            "Table Viewer"
        }

        fn ui(&mut self, ui: &mut egui::Ui, _ctx: &Context, qe: &Arc<QueryEngine>) {
            // Refresh table list
            if let Ok(snapshot) = qe.snapshot() {
                self.table_names = snapshot.table_names();
            }

            // Table selector
            ui.horizontal(|ui| {
                ui.label("Table:");
                let mut current = self.selected_table.clone().unwrap_or_default();
                let response = egui::ComboBox::from_id_salt("table_selector")
                    .selected_text(&current)
                    .show_ui(ui, |ui| {
                        for name in &self.table_names {
                            if ui
                                .selectable_value(&mut current, name.clone(), name)
                                .changed()
                            {
                                self.selected_table = Some(name.clone());
                                self.needs_reload = true;
                            }
                        }
                    })
                    .response;
                // response.changed() is already handled via selectable_value above
                let _ = response;

                if ui.button("Reload").clicked() {
                    self.needs_reload = true;
                }

                ui.label(format!(
                    "{} rows (max {})",
                    self.row_data.len(),
                    self.max_rows
                ));
            });

            ui.separator();

            // Reload data if needed
            if self.needs_reload {
                self.reload(qe);
            }

            // Data grid
            if self.selected_table.is_some() {
                egui::ScrollArea::both().show(ui, |ui| {
                    egui::Grid::new("table_grid").striped(true).show(ui, |ui| {
                        // Header row
                        for col in &self.columns {
                            ui.label(egui::RichText::new(col).strong());
                        }
                        ui.end_row();

                        // Data rows
                        for row in &self.row_data {
                            for cell in row {
                                ui.label(cell);
                            }
                            ui.end_row();
                        }
                    });
                });
            } else {
                ui.label("Select a table to view data.");
            }
        }
    }
}

#[cfg(not(debug_assertions))]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::Context;
    use scharnhorst_query::QueryEngine;
    use std::sync::Arc;

    pub struct TablePanel;

    impl Default for TablePanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TablePanel {
        pub fn new() -> Self {
            Self
        }
    }

    impl InspectorPanel for TablePanel {
        fn name(&self) -> &str {
            "Table Viewer"
        }
        fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &Context, _qe: &Arc<QueryEngine>) {}
    }
}

pub use inner::TablePanel;
