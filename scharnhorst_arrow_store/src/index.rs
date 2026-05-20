use std::collections::HashMap;

use scharnhorst_core::{RowId, Tick};

/// Maps a primary key value to its row location within a specific tick's batches.
#[derive(Debug, Clone, Default)]
pub struct PrimaryKeyIndex {
    /// key -> (tick, batch_index, row_index)
    entries: HashMap<String, Vec<(Tick, usize, usize)>>,
}

impl PrimaryKeyIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, key: String, tick: Tick, batch_idx: usize, row_idx: usize) {
        self.entries
            .entry(key)
            .or_default()
            .push((tick, batch_idx, row_idx));
    }

    pub fn lookup(&self, key: &str) -> Option<&Vec<(Tick, usize, usize)>> {
        self.entries.get(key)
    }

    pub fn lookup_latest(&self, key: &str) -> Option<(Tick, usize, usize)> {
        self.entries
            .get(key)
            .and_then(|v| v.iter().copied().max_by_key(|&(tick, _, _)| tick))
    }

    pub fn remove_key(&mut self, key: &str) -> Option<Vec<(Tick, usize, usize)>> {
        self.entries.remove(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Maps a foreign key value to the set of rows that reference it.
#[derive(Debug, Clone)]
pub struct ForeignKeyIndex {
    pub column_name: String,
    pub target_table: String,
    /// target_key -> [(tick, batch_index, row_index)]
    entries: HashMap<String, Vec<(Tick, usize, usize)>>,
}

impl ForeignKeyIndex {
    pub fn new(column_name: impl Into<String>, target_table: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            target_table: target_table.into(),
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, tick: Tick, batch_idx: usize, row_idx: usize) {
        self.entries
            .entry(key)
            .or_default()
            .push((tick, batch_idx, row_idx));
    }

    pub fn lookup(&self, key: &str) -> Option<&Vec<(Tick, usize, usize)>> {
        self.entries.get(key)
    }

    pub fn lookup_latest(&self, key: &str) -> Option<(Tick, usize, usize)> {
        self.entries
            .get(key)
            .and_then(|v| v.iter().copied().max_by_key(|&(tick, _, _)| tick))
    }

    pub fn remove_key(&mut self, key: &str) -> Option<Vec<(Tick, usize, usize)>> {
        self.entries.remove(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A lightweight handle returned by index lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowLocation {
    pub tick: Tick,
    pub batch_index: usize,
    pub row_index: usize,
    pub row_id: RowId,
}
