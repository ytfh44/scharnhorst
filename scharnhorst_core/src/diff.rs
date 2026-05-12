use serde::{Deserialize, Serialize};

use crate::id::RowId;

/// A single atomic change to a row or table.
///
/// Diffs are produced by simulation systems and accumulated during a tick.
/// They are applied atomically at tick end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Diff {
 /// Update an existing row's column value.
    Update {
        table: String,
        row: RowId,
        column: String,
 /// JSON-encoded new value.
        value: serde_json::Value,
    },
 /// Insert a new row.
    Insert {
        table: String,
        row: RowId,
 /// JSON-encoded row values keyed by column name.
        values: serde_json::Map<String, serde_json::Value>,
    },
 /// Delete an existing row.
    Delete {
        table: String,
        row: RowId,
    },
 /// Replace an entire table's contents (e.g. rebuild per tick).
    ReplaceTable {
        table: String,
 /// JSON-encoded rows.
        rows: Vec<serde_json::Map<String, serde_json::Value>>,
    },
}

impl Diff {
 /// Return the name of the table affected by this diff.
    pub fn table(&self) -> &str {
        match self {
            Diff::Update { table, .. } => table,
            Diff::Insert { table, .. } => table,
            Diff::Delete { table, .. } => table,
            Diff::ReplaceTable { table, .. } => table,
        }
    }

 /// Return the row id affected by this diff, if any.
    pub fn row_id(&self) -> Option<RowId> {
        match self {
            Diff::Update { row, .. } => Some(*row),
            Diff::Insert { row, .. } => Some(*row),
            Diff::Delete { row, .. } => Some(*row),
            Diff::ReplaceTable { .. } => None,
        }
    }
}

/// A batch of diffs produced by a single system or source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffBatch {
 /// Optional label identifying the producer (system name, etc.).
    pub source: String,
 /// The diffs themselves.
    pub diffs: Vec<Diff>,
}

impl DiffBatch {
 /// Create a new batch with the given source label.
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            diffs: Vec::new(),
        }
    }

 /// Append a diff to this batch.
    pub fn push(&mut self, diff: Diff) {
        self.diffs.push(diff);
    }

 /// Return an iterator over the diffs in this batch.
    pub fn iter(&self) -> impl Iterator<Item = &Diff> {
        self.diffs.iter()
    }

 /// Return the number of diffs in this batch.
    pub fn len(&self) -> usize {
        self.diffs.len()
    }

 /// Return true if this batch contains no diffs.
    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }
}
