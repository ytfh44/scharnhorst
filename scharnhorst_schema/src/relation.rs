use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::error::{SchemaError, SchemaResult};

/// The cardinality / kind of a relationship between two tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RelationKind {
 /// One-to-many: one row in `from` may relate to many rows in `to`.
    OneToMany,
 /// Many-to-many: rows in both directions may have multiple links.
    ManyToMany,
 /// Composition: rows in `to` are owned by a row in `from`.
    Composition,
}

/// A directed edge in the relation graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationEdge {
    pub from: String,
    pub to: String,
    pub kind: RelationKind,
 /// Column in `from` that stores the foreign key (REQUIRED).
    pub from_column: String,
 /// Column in `to` that stores the foreign key (optional, falls back to target's primary key).
    pub to_column: Option<String>,
}

/// A graph of relations between tables.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationGraph {
    edges: Vec<RelationEdge>,
    adjacency: HashMap<String, Vec<usize>>,
}

impl RelationGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_edge(&mut self, edge: RelationEdge) -> SchemaResult<()> {
        let key = Self::edge_key(&edge.from, &edge.to);
        if self.find_edge(&edge.from, &edge.to).is_some() {
            return Err(SchemaError::RelationAlreadyExists {
                from: edge.from.clone(),
                to: edge.to.clone(),
            });
        }

        if self.would_create_cycle(&edge.from, &edge.to) {
            return Err(SchemaError::CircularRelation(format!(
                "{} -> {}",
                edge.from, edge.to
            )));
        }

        let idx = self.edges.len();
        self.edges.push(edge);
        self.adjacency
            .entry(key.0)
            .or_default()
            .push(idx);
        Ok(())
    }

    pub fn remove_edge(&mut self, from: &str, to: &str) -> SchemaResult<()> {
        let pos = self
            .edges
            .iter()
            .position(|e| e.from == from && e.to == to)
            .ok_or_else(|| SchemaError::RelationNotFound {
                from: from.to_owned(),
                to: to.to_owned(),
            })?;
        self.edges.remove(pos);
        self.rebuild_adjacency();
        Ok(())
    }

    pub fn find_edge(&self, from: &str, to: &str) -> Option<&RelationEdge> {
        self.edges.iter().find(|e| e.from == from && e.to == to)
    }

    pub fn edges_from(&self, table: &str) -> impl Iterator<Item = &RelationEdge> {
        self.adjacency
            .get(table)
            .into_iter()
            .flat_map(move |indices| indices.iter().filter_map(move |&i| self.edges.get(i)))
    }

    pub fn all_edges(&self) -> &[RelationEdge] {
        &self.edges
    }

    pub fn tables(&self) -> HashSet<String> {
        self.edges
            .iter()
            .flat_map(|e| [e.from.clone(), e.to.clone()])
            .collect()
    }

    fn edge_key(from: &str, to: &str) -> (String, String) {
        (from.to_owned(), to.to_owned())
    }

    fn rebuild_adjacency(&mut self) {
        self.adjacency.clear();
        for (idx, edge) in self.edges.iter().enumerate() {
            self.adjacency
                .entry(edge.from.clone())
                .or_default()
                .push(idx);
        }
    }

    fn would_create_cycle(&self, start: &str, target: &str) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![target];
        while let Some(current) = stack.pop() {
            if current == start {
                return true;
            }
            if !visited.insert(current.to_owned()) {
                continue;
            }
            for edge in self.edges_from(current) {
                stack.push(&edge.to);
            }
        }
        false
    }

 /// Performs global cycle detection using DFS with three-color marking.
 /// Returns a list of cycle paths if cycles are found.
    pub fn detect_cycles(&self) -> Vec<Vec<String>> {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Color { White, Gray, Black }
        use std::collections::HashMap;

        let all_tables: Vec<String> = self.tables().into_iter().collect();
        let mut colors: HashMap<String, Color> = all_tables.iter().map(|t| (t.clone(), Color::White)).collect();
        let mut cycles = Vec::new();
        let mut path = Vec::new();

        for start_node in &all_tables {
            if !matches!(colors.get(start_node), Some(Color::White)) {
                continue;
            }
            let mut stack: Vec<(bool, String)> = vec![(true, start_node.clone())];
            
            while let Some((is_entering, node)) = stack.pop() {
                if is_entering {
                    match colors.get(&node) {
                        Some(Color::Black) => continue,
                        Some(Color::Gray) => {
                            let cycle_start = path.iter().position(|n| n == &node).unwrap_or(0);
                            cycles.push(path[cycle_start..].to_vec());
                            continue;
                        }
                        _ => {}
                    }
                    colors.insert(node.clone(), Color::Gray);
                    path.push(node.clone());
                    stack.push((false, node.clone()));
                    for edge in self.edges_from(&node) {
                        if matches!(colors.get(&edge.to), Some(Color::White)) {
                            stack.push((true, edge.to.clone()));
                        }
                    }
                } else {
                    path.pop();
                    colors.insert(node, Color::Black);
                }
            }
        }

        cycles
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(from: &str, to: &str) -> RelationEdge {
        RelationEdge {
            from: from.to_string(),
            to: to.to_string(),
            kind: RelationKind::OneToMany,
            from_column: "id".to_string(),
            to_column: None,
        }
    }

    fn composition_edge(from: &str, to: &str) -> RelationEdge {
        RelationEdge {
            from: from.to_string(),
            to: to.to_string(),
            kind: RelationKind::Composition,
            from_column: "owner_id".to_string(),
            to_column: None,
        }
    }

 // ---- add_edge ----

    #[test]
    fn add_edge_ok() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        g.add_edge(edge("B", "C"))?;
        assert_eq!(g.all_edges().len(), 2);
        Ok(())
    }

    #[test]
    fn add_duplicate_edge_is_err() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        let result = g.add_edge(edge("A", "B"));
        assert_eq!(
            result,
            Err(SchemaError::RelationAlreadyExists {
                from: "A".to_string(),
                to: "B".to_string()
            })
        );
        Ok(())
    }

    #[test]
    fn cycle_detection_direct() {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B")).unwrap();
        let result = g.add_edge(edge("B", "A"));
        assert!(matches!(result, Err(SchemaError::CircularRelation(_))));
    }

    #[test]
    fn cycle_detection_indirect() {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B")).unwrap();
        g.add_edge(edge("B", "C")).unwrap();
        let result = g.add_edge(edge("C", "A"));
        assert!(matches!(result, Err(SchemaError::CircularRelation(_))));
    }

    #[test]
    fn cycle_detection_no_false_positive() -> SchemaResult<()> {
 // A -> B, A -> C, B -> D, C -> D should have no cycle
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        g.add_edge(edge("A", "C"))?;
        g.add_edge(edge("B", "D"))?;
        g.add_edge(edge("C", "D"))?;
        assert_eq!(g.all_edges().len(), 4);
        Ok(())
    }

 // ---- remove_edge ----

    #[test]
    fn remove_edge_ok() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        g.add_edge(edge("B", "C"))?;
        g.remove_edge("A", "B")?;
        assert_eq!(g.all_edges().len(), 1);
        assert!(g.find_edge("A", "B").is_none());
        Ok(())
    }

    #[test]
    fn remove_nonexistent_edge_is_err() {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B")).unwrap();
        let result = g.remove_edge("X", "Y");
        assert!(matches!(result, Err(SchemaError::RelationNotFound { .. })));
    }

 // ---- find_edge ----

    #[test]
    fn find_edge_found() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(composition_edge("Parent", "Child"))?;
        let found = g.find_edge("Parent", "Child").unwrap();
        assert_eq!(found.kind, RelationKind::Composition);
        assert_eq!(found.from_column, "owner_id".to_string());
        Ok(())
    }

    #[test]
    fn find_edge_not_found() {
        let g = RelationGraph::new();
        assert!(g.find_edge("A", "B").is_none());
    }

 // ---- edges_from ----

    #[test]
    fn edges_from_multiple() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        g.add_edge(edge("A", "C"))?;
        g.add_edge(edge("B", "C"))?;
        let from_a: Vec<&RelationEdge> = g.edges_from("A").collect();
        assert_eq!(from_a.len(), 2);
        let from_b: Vec<&RelationEdge> = g.edges_from("B").collect();
        assert_eq!(from_b.len(), 1);
        Ok(())
    }

    #[test]
    fn edges_from_none() {
        let g = RelationGraph::new();
        let edges: Vec<&RelationEdge> = g.edges_from("X").collect();
        assert!(edges.is_empty());
    }

 // ---- tables ----

    #[test]
    fn tables_collects_all() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("A", "B"))?;
        g.add_edge(edge("B", "C"))?;
        let tables = g.tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains("A"));
        assert!(tables.contains("B"));
        assert!(tables.contains("C"));
        Ok(())
    }

    #[test]
    fn tables_empty() {
        let g = RelationGraph::new();
        assert!(g.tables().is_empty());
    }

 // ---- all edges ----

    #[test]
    fn all_edges_maintains_order() -> SchemaResult<()> {
        let mut g = RelationGraph::new();
        g.add_edge(edge("X", "Y"))?;
        g.add_edge(edge("Y", "Z"))?;
        let all = g.all_edges();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].from, "X");
        assert_eq!(all[1].from, "Y");
        Ok(())
    }
}
