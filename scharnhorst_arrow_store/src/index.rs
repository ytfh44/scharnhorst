use std::collections::HashMap;

use scharnhorst_core::{RowId, Tick};

/// Maps a primary key value to its row location within a specific tick's batches.
///
/// Uses a layered structure for performance:
/// - **Hot layer** (`latest_entries`): O(1) lookup for the latest version of each key.
/// - **Cold layer** (`historical_entries`): Optional full history, enabled via
///   [`with_history`](Self::with_history). When disabled, [`lookup`](Self::lookup)
///   returns `None`.
///
/// # Invariants
///
/// - The key set of `latest_entries` equals the key set of `historical_entries`
///   (when history is enabled).
/// - For any key, the entry in `latest_entries` has the maximum tick among all
///   entries for that key.
/// - `remove_key` clears both layers simultaneously.
#[derive(Debug, Clone)]
pub struct PrimaryKeyIndex {
    /// Hot layer: key -> single latest (tick, batch_index, row_index).
    latest_entries: HashMap<String, (Tick, usize, usize)>,
    /// Cold layer: key -> full history vector (disabled when `None`).
    historical_entries: Option<HashMap<String, Vec<(Tick, usize, usize)>>>,
}

impl PrimaryKeyIndex {
    /// Create an empty index with history enabled (backwards-compatible default).
    pub fn new() -> Self {
        Self {
            latest_entries: HashMap::new(),
            historical_entries: Some(HashMap::new()),
        }
    }

    /// Create an empty index with history optionally enabled.
    ///
    /// When `enabled` is `false`, [`lookup`](Self::lookup) always returns `None`,
    /// and no historical data is stored, saving memory.
    /// [`lookup_latest`](Self::lookup_latest) works regardless of this setting.
    pub fn with_history(enabled: bool) -> Self {
        Self {
            latest_entries: HashMap::new(),
            historical_entries: if enabled {
                Some(HashMap::new())
            } else {
                None
            },
        }
    }

    /// Insert an index entry for a key at the given position.
    ///
    /// Updates both the hot layer (if the new tick is >= current) and the cold
    /// layer (if history is enabled).
    pub fn insert(&mut self, key: String, tick: Tick, batch_idx: usize, row_idx: usize) {
        // Hot layer: only update if this tick is newer or no entry exists
        let should_update = match self.latest_entries.get(&key) {
            Some(&(existing_tick, _, _)) => tick >= existing_tick,
            None => true,
        };
        if should_update {
            self.latest_entries.insert(key.clone(), (tick, batch_idx, row_idx));
        }

        // Cold layer: append if history is enabled
        if let Some(ref mut hist) = self.historical_entries {
            hist.entry(key).or_default().push((tick, batch_idx, row_idx));
        }
    }

    /// Look up the full history for a key.
    ///
    /// Returns `None` if history is disabled or the key is not found.
    pub fn lookup(&self, key: &str) -> Option<&Vec<(Tick, usize, usize)>> {
        self.historical_entries.as_ref()?.get(key)
    }

    /// Look up the latest entry for a key in O(1).
    pub fn lookup_latest(&self, key: &str) -> Option<(Tick, usize, usize)> {
        self.latest_entries.get(key).copied()
    }

    /// Remove all entries (both layers) for a key.
    pub fn remove_key(&mut self, key: &str) -> Option<Vec<(Tick, usize, usize)>> {
        self.latest_entries.remove(key);
        self.historical_entries
            .as_mut()
            .and_then(|hist| hist.remove(key))
    }

    /// Iterate over all keys present in the index.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.latest_entries.keys().map(|s| s.as_str())
    }

    /// Returns true if the index contains no entries.
    pub fn is_empty(&self) -> bool {
        self.latest_entries.is_empty()
    }

    /// Returns the number of unique keys in the index.
    pub fn len(&self) -> usize {
        self.latest_entries.len()
    }

    /// Returns true if history tracking is enabled.
    pub fn history_enabled(&self) -> bool {
        self.historical_entries.is_some()
    }
}

impl Default for PrimaryKeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Maps a foreign key value to the set of rows that reference it.
///
/// Uses the same layered structure as [`PrimaryKeyIndex`] for O(1)
/// [`lookup_latest`](Self::lookup_latest) performance and optional
/// history tracking via [`with_history`](Self::with_history).
#[derive(Debug, Clone)]
pub struct ForeignKeyIndex {
    pub column_name: String,
    pub target_table: String,
    /// Hot layer: key -> single latest (tick, batch_index, row_index).
    latest_entries: HashMap<String, (Tick, usize, usize)>,
    /// Cold layer: key -> full history vector (disabled when `None`).
    historical_entries: Option<HashMap<String, Vec<(Tick, usize, usize)>>>,
}

impl ForeignKeyIndex {
    /// Create an empty foreign key index with history enabled.
    pub fn new(column_name: impl Into<String>, target_table: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            target_table: target_table.into(),
            latest_entries: HashMap::new(),
            historical_entries: Some(HashMap::new()),
        }
    }

    /// Create an empty foreign key index with history optionally enabled.
    ///
    /// When `enabled` is `false`, [`lookup`](Self::lookup) always returns `None`.
    pub fn with_history(
        column_name: impl Into<String>,
        target_table: impl Into<String>,
        enabled: bool,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            target_table: target_table.into(),
            latest_entries: HashMap::new(),
            historical_entries: if enabled {
                Some(HashMap::new())
            } else {
                None
            },
        }
    }

    /// Insert an index entry for a key at the given position.
    pub fn insert(&mut self, key: String, tick: Tick, batch_idx: usize, row_idx: usize) {
        let should_update = match self.latest_entries.get(&key) {
            Some(&(existing_tick, _, _)) => tick >= existing_tick,
            None => true,
        };
        if should_update {
            self.latest_entries.insert(key.clone(), (tick, batch_idx, row_idx));
        }

        if let Some(ref mut hist) = self.historical_entries {
            hist.entry(key).or_default().push((tick, batch_idx, row_idx));
        }
    }

    /// Look up the full history for a key.
    ///
    /// Returns `None` if history is disabled or the key is not found.
    pub fn lookup(&self, key: &str) -> Option<&Vec<(Tick, usize, usize)>> {
        self.historical_entries.as_ref()?.get(key)
    }

    /// Look up the latest entry for a key in O(1).
    pub fn lookup_latest(&self, key: &str) -> Option<(Tick, usize, usize)> {
        self.latest_entries.get(key).copied()
    }

    /// Remove all entries (both layers) for a key.
    pub fn remove_key(&mut self, key: &str) -> Option<Vec<(Tick, usize, usize)>> {
        self.latest_entries.remove(key);
        self.historical_entries
            .as_mut()
            .and_then(|hist| hist.remove(key))
    }

    /// Iterate over all keys present in the index.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.latest_entries.keys().map(|s| s.as_str())
    }

    /// Returns true if the index contains no entries.
    pub fn is_empty(&self) -> bool {
        self.latest_entries.is_empty()
    }

    /// Returns the number of unique keys in the index.
    pub fn len(&self) -> usize {
        self.latest_entries.len()
    }

    /// Returns true if history tracking is enabled.
    pub fn history_enabled(&self) -> bool {
        self.historical_entries.is_some()
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