//! Relation graph visualizer: DAG with table-name-labeled nodes.

#[cfg(debug_assertions)]
mod inner {
    use super::super::super::inspector::InspectorPanel;
    use egui::{
        Align2, Color32, Context, CornerRadius, FontId, Pos2, Rect, Sense, Stroke, StrokeKind, Vec2,
    };
    use scharnhorst_query::QueryEngine;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// A node in the relation graph = a table.
    pub struct GraphNode {
        pub name: String,
        pub pos: Pos2,
        pub size: Vec2, // auto-computed based on text
    }

    /// An edge in the relation graph = "from" table -> "to" table.
    pub struct GraphEdge {
        pub from: String,
        pub to: String,
        pub relation_name: String, // the name of the relation (e.g., "fk_hero_id")
    }

    pub struct RelationGraphPanel {
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        needs_reload: bool,
        /// Drag state
        dragging: Option<usize>, // node index being dragged
        drag_offset: Vec2,
    }

    impl Default for RelationGraphPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RelationGraphPanel {
        pub fn new() -> Self {
            Self {
                nodes: Vec::new(),
                edges: Vec::new(),
                needs_reload: true,
                dragging: None,
                drag_offset: Vec2::ZERO,
            }
        }

        pub fn nodes(&self) -> &[GraphNode] {
            &self.nodes
        }

        pub fn edges(&self) -> &[GraphEdge] {
            &self.edges
        }

        pub fn reload(&mut self, qe: &Arc<QueryEngine>) {
            self.nodes.clear();
            self.edges.clear();

            let Ok(snapshot) = qe.snapshot() else {
                return;
            };
            let table_names = snapshot.table_names();

            // Collect edges from schema registry's relation graph (if available)
            let mut has_registry_edges = false;
            if let Ok(reg) = qe.schema_registry() {
                let graph = reg.relation_graph();
                for edge in graph.all_edges().iter() {
                    if table_names.contains(&edge.from) && table_names.contains(&edge.to) {
                        self.edges.push(GraphEdge {
                            from: edge.from.clone(),
                            to: edge.to.clone(),
                            relation_name: format!("{} ({:?})", edge.from_column, edge.kind),
                        });
                        has_registry_edges = true;
                    }
                }
            }

            // If no registry edges, fall back to FK inference from column naming conventions
            if !has_registry_edges {
                for table_name in &table_names {
                    let Ok(columns) = snapshot.column_names(table_name) else {
                        continue;
                    };
                    for col in &columns {
                        // Detect FK columns: those starting with "fk_" or ending with "_id"
                        let target = if let Some(t) = col.strip_prefix("fk_") {
                            t.to_owned()
                        } else if let Some(t) = col.strip_suffix("_id") {
                            t.to_owned()
                        } else {
                            continue;
                        };
                        // Only add edge if target table exists and is not self-referencing
                        if table_names.contains(&target) && target != *table_name {
                            self.edges.push(GraphEdge {
                                from: table_name.clone(),
                                to: target,
                                relation_name: col.clone(),
                            });
                        }
                    }
                }
            }

            // Place nodes in a circular layout initially
            let n = table_names.len();
            if n == 0 {
                self.needs_reload = false;
                return;
            }

            let radius = 150.0_f32;
            let cx = 300.0_f32;
            let cy = 300.0_f32;

            for (i, name) in table_names.iter().enumerate() {
                let angle = (i as f32) * (2.0_f32 * std::f32::consts::PI) / (n as f32);
                let x = cx + radius * angle.cos();
                let y = cy + radius * angle.sin();
                self.nodes.push(GraphNode {
                    name: name.clone(),
                    pos: Pos2::new(x, y),
                    size: Vec2::new(120.0, 30.0),
                });
            }

            self.needs_reload = false;
        }

        fn draw_arrow(
            painter: &egui::Painter,
            from: Pos2,
            to: Pos2,
            color: Color32,
            thickness: f32,
        ) {
            let dir = (to - from).normalized();
            let arrow_size = 8.0_f32;
            let perp = Vec2::new(-dir.y, dir.x);
            let arrow_base = to - dir * arrow_size;
            let arrow_left = arrow_base + perp * arrow_size * 0.5;
            let arrow_right = arrow_base - perp * arrow_size * 0.5;

            // Line
            painter.line_segment([from, to], Stroke::new(thickness, color));
            // Arrow head
            painter.add(egui::Shape::convex_polygon(
                vec![to, arrow_left, arrow_right],
                color,
                Stroke::NONE,
            ));
        }
    }

    impl InspectorPanel for RelationGraphPanel {
        fn name(&self) -> &str {
            "Relation Graph"
        }

        fn ui(&mut self, ui: &mut egui::Ui, _ctx: &Context, qe: &Arc<QueryEngine>) {
            // Controls
            ui.horizontal(|ui| {
                if ui.button("Reload").clicked() {
                    self.needs_reload = true;
                }
                ui.label(format!(
                    "{} nodes, {} edges",
                    self.nodes.len(),
                    self.edges.len()
                ));
            });

            ui.separator();

            if self.needs_reload {
                self.reload(qe);
            }

            if self.nodes.is_empty() {
                ui.label("No tables found. Load a schema first.");
                return;
            }

            // Canvas for graph rendering
            let available = ui.available_size();
            let (response, painter) = ui.allocate_painter(available, Sense::click_and_drag());

            // Check for canvas drag
            if response.dragged() {
                if let Some(idx) = self.dragging {
                    self.nodes[idx].pos += response.drag_delta();
                }
            }

            // Draw edges
            let node_positions: HashMap<String, Pos2> =
                self.nodes.iter().map(|n| (n.name.clone(), n.pos)).collect();

            for edge in &self.edges {
                if let (Some(&from), Some(&to)) =
                    (node_positions.get(&edge.from), node_positions.get(&edge.to))
                {
                    let color = Color32::from_gray(180);
                    Self::draw_arrow(&painter, from, to, color, 1.5);
                }
            }

            // Draw nodes
            let mut drag_updates: Vec<(usize, Pos2)> = Vec::new();
            for (i, node) in self.nodes.iter().enumerate() {
                let rect = Rect::from_center_size(node.pos, node.size);
                let node_response = ui.allocate_rect(rect, Sense::click_and_drag());

                // Node rectangle
                let fill_color = if self.dragging == Some(i) {
                    Color32::from_rgb(80, 120, 200)
                } else {
                    Color32::from_rgb(60, 80, 140)
                };
                painter.rect_filled(rect, CornerRadius::same(5), fill_color);
                painter.rect_stroke(
                    rect,
                    CornerRadius::same(5),
                    Stroke::new(1.0_f32, Color32::WHITE),
                    StrokeKind::Inside,
                );

                // Node label
                painter.text(
                    rect.center(),
                    Align2::CENTER_CENTER,
                    &node.name,
                    FontId::proportional(14.0),
                    Color32::WHITE,
                );

                // Handle drag start
                if node_response.drag_started() {
                    self.dragging = Some(i);
                    if let Some(pointer_pos) = node_response.interact_pointer_pos() {
                        self.drag_offset = node.pos - pointer_pos;
                    }
                }

                // Collect drag update (apply after immutable borrow ends)
                if node_response.dragged() && self.dragging == Some(i) {
                    if let Some(pointer_pos) = node_response.interact_pointer_pos() {
                        drag_updates.push((i, pointer_pos + self.drag_offset));
                    }
                }
            }

            // Apply collected drag position updates
            for (i, new_pos) in drag_updates {
                self.nodes[i].pos = new_pos;
            }

            // Release drag if mouse released
            if response.drag_stopped() {
                self.dragging = None;
            }

            // Draw edge labels
            for edge in &self.edges {
                if let (Some(&from), Some(&to)) =
                    (node_positions.get(&edge.from), node_positions.get(&edge.to))
                {
                    let mid = Pos2::new((from.x + to.x) / 2.0, (from.y + to.y) / 2.0);
                    painter.text(
                        mid + Vec2::new(0.0, -8.0),
                        Align2::CENTER_BOTTOM,
                        &edge.relation_name,
                        FontId::proportional(10.0),
                        Color32::from_gray(200),
                    );
                }
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

    pub struct RelationGraphPanel;

    impl Default for RelationGraphPanel {
        fn default() -> Self {
            Self::new()
        }
    }

    impl RelationGraphPanel {
        pub fn new() -> Self {
            Self
        }
    }

    impl InspectorPanel for RelationGraphPanel {
        fn name(&self) -> &str {
            "Relation Graph"
        }
        fn ui(&mut self, _ui: &mut egui::Ui, _ctx: &Context, _qe: &Arc<QueryEngine>) {}
    }
}

pub use inner::RelationGraphPanel;
