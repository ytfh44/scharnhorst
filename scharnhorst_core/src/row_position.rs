use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::id::RowId;

/// Maps RowId values to their physical storage positions within a table.
///
/// RowId.0 is opaque and MUST NOT be used as an array index.
/// Always resolve through this map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowPositionMap {
    entries: HashMap<u64, (usize, usize)>, // RowId.0 鈫?(batch_idx, row_offset)
}

impl RowPositionMap {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, row_id: RowId, batch_index: usize, row_offset: usize) {
        self.entries.insert(row_id.as_u64(), (batch_index, row_offset));
    }

    pub fn remove(&mut self, row_id: &RowId) -> Option<(usize, usize)> {
        self.entries.remove(&row_id.as_u64())
    }

    pub fn position_of(&self, row_id: RowId) -> Option<(usize, usize)> {
        self.entries.get(&row_id.as_u64()).copied()
    }

    pub fn contains(&self, row_id: RowId) -> bool {
        self.entries.contains_key(&row_id.as_u64())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

 /// Returns an iterator over all RowIds currently stored in the position map.
    pub fn row_ids(&self) -> impl Iterator<Item = RowId> + '_ {
        self.entries.keys().map(|&raw| RowId::new(raw))
    }
}

/// Provides typed column access for a single row identified by RowId.
///
/// Returned by [`QueryEngine::lookup_row`]. The physical (batch_idx, offset)
/// is resolved internally; callers never see raw array indices.
#[derive(Debug, Clone)]
pub struct RowLookup {
    row_id: RowId,
    batch_index: usize,
    row_offset: usize,
}

impl RowLookup {
    pub fn new(row_id: RowId, batch_index: usize, row_offset: usize) -> Self {
        Self {
            row_id,
            batch_index,
            row_offset,
        }
    }

    pub fn row_id(&self) -> RowId {
        self.row_id
    }

    pub fn batch_index(&self) -> usize {
        self.batch_index
    }

    pub fn row_offset(&self) -> usize {
        self.row_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::RowId;

    #[test]
    fn new_map_is_empty() {
        let map = RowPositionMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn default_map_is_empty() {
        let map = RowPositionMap::default();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn insert_and_position_of() {
        let mut map = RowPositionMap::new();
        let row = RowId(42);
        map.insert(row, 1, 3);
        assert!(!map.is_empty());
        assert_eq!(map.len(), 1);
        assert_eq!(map.position_of(row), Some((1, 3)));
    }

    #[test]
    fn insert_overwrites_existing() {
        let mut map = RowPositionMap::new();
        let row = RowId(7);
        map.insert(row, 0, 0);
        map.insert(row, 2, 5);
        assert_eq!(map.len(), 1);
        assert_eq!(map.position_of(row), Some((2, 5)));
    }

    #[test]
    fn remove_deleted_row() {
        let mut map = RowPositionMap::new();
        let row = RowId(99);
        map.insert(row, 0, 1);
        let removed = map.remove(&row);
        assert_eq!(removed, Some((0, 1)));
        assert!(map.is_empty());
        assert_eq!(map.position_of(row), None);
    }

    #[test]
    fn remove_nonexistent_returns_none() {
        let mut map = RowPositionMap::new();
        let row = RowId(999);
        assert_eq!(map.remove(&row), None);
    }

    #[test]
    fn contains_checks_membership() {
        let mut map = RowPositionMap::new();
        let a = RowId(1);
        let b = RowId(2);
        map.insert(a, 0, 0);
        assert!(map.contains(a));
        assert!(!map.contains(b));
    }

    #[test]
    fn clear_removes_all() {
        let mut map = RowPositionMap::new();
        map.insert(RowId(1), 0, 0);
        map.insert(RowId(2), 1, 1);
        map.clear();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn multiple_rows_independent() {
        let mut map = RowPositionMap::new();
        map.insert(RowId(10), 0, 0);
        map.insert(RowId(20), 0, 1);
        map.insert(RowId(30), 1, 0);
        assert_eq!(map.len(), 3);
        assert_eq!(map.position_of(RowId(10)), Some((0, 0)));
        assert_eq!(map.position_of(RowId(20)), Some((0, 1)));
        assert_eq!(map.position_of(RowId(30)), Some((1, 0)));
        assert_eq!(map.position_of(RowId(99)), None);
    }

    #[test]
    fn row_lookup_construction() {
        let lookup = RowLookup::new(RowId(42), 2, 7);
        assert_eq!(lookup.row_id(), RowId(42));
        assert_eq!(lookup.batch_index(), 2);
        assert_eq!(lookup.row_offset(), 7);
    }

    #[test]
    fn row_lookup_clone() {
        let a = RowLookup::new(RowId(1), 0, 0);
        let b = a.clone();
        assert_eq!(a.row_id(), b.row_id());
        assert_eq!(a.batch_index(), b.batch_index());
        assert_eq!(a.row_offset(), b.row_offset());
    }

    #[test]
    fn row_position_map_debug() {
        let map = RowPositionMap::new();
        let debug = format!("{:?}", map);
        assert!(debug.starts_with("RowPositionMap"));
    }

    #[test]
    fn row_position_map_serialize_roundtrip() {
        let mut map = RowPositionMap::new();
        map.insert(RowId(1), 0, 0);
        map.insert(RowId(2), 1, 5);
        let json = serde_json::to_string(&map).unwrap();
        let back: RowPositionMap = serde_json::from_str(&json).unwrap();
        assert_eq!(map, back);
    }
}
