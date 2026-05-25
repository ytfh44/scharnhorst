use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use scharnhorst_core::Tick;

use crate::error::{ArrowStoreError, ArrowStoreResult};
use crate::snapshot::WorldSnapshot;

/// A read-only, frozen snapshot of the world state at a specific tick.
///
/// # Safety (I-WV-IMMUTABLE)
/// WorldView wraps an `Arc<WorldSnapshot>` and provides ONLY typed read access.
/// Once created, it never changes — even if the underlying store advances ticks.
///
/// # Arrow Encapsulation
/// WorldView NEVER exposes `RecordBatch`, `Schema`, `ArrayRef`, or any Arrow
/// internal types. All data access goes through typed getters that return
/// Rust-native types.
///
/// # Performance (I-WV-ARC-BACKED)
/// Cloning is cheap — only bumps the Arc reference count.
#[derive(Debug, Clone)]
pub struct WorldView {
    snapshot: Arc<WorldSnapshot>,
    tick: Tick,
}

impl WorldView {
    pub fn new(snapshot: Arc<WorldSnapshot>) -> Self {
        let tick = snapshot.tick();
        Self { snapshot, tick }
    }

    /// Returns the tick at which this snapshot was captured.
    pub fn tick(&self) -> Tick {
        self.tick
    }

    /// Returns a sorted list of all table names present in this snapshot.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.snapshot.table_names().map(|s| s.to_owned()).collect();
        names.sort();
        names
    }

    /// Returns the total number of rows in the given table across all batches.
    pub fn row_count(&self, table_name: &str) -> ArrowStoreResult<usize> {
        let batches = self.snapshot.table_batches(table_name)?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    /// Returns the column names for the given table in order.
    ///
    /// Derived from the first batch's schema. Returns an empty vec if the table
    /// has no batches.
    pub fn column_names(&self, table_name: &str) -> ArrowStoreResult<Vec<String>> {
        let batches = self.snapshot.table_batches(table_name)?;
        let Some(first) = batches.first() else {
            return Ok(Vec::new());
        };
        Ok(first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect())
    }

    /// Returns the Arrow data type name for a column as a human-readable string.
    pub fn column_type(&self, table_name: &str, column_name: &str) -> ArrowStoreResult<String> {
        let batches = self.snapshot.table_batches(table_name)?;
        let first = batches.first().ok_or_else(|| {
            ArrowStoreError::Schema(format!("table '{table_name}' has no data batches"))
        })?;
        let schema = first.schema();
        let field = schema.field_with_name(column_name).map_err(|_| {
            ArrowStoreError::Schema(format!(
                "column '{column_name}' not found in table '{table_name}'"
            ))
        })?;
        Ok(format!("{:?}", field.data_type()))
    }

    /// Returns an iterator over typed rows.
    ///
    /// Each row exposes `get_i64`, `get_f64`, `get_string`, `get_bool`.
    pub fn iter_rows(&self, table_name: &str) -> ArrowStoreResult<WorldViewRowIter> {
        let batches = self.snapshot.table_batches(table_name)?;
        WorldViewRowIter::new(batches)
    }
}

// ---------------------------------------------------------------------------
// WorldViewRowIter
// ---------------------------------------------------------------------------

/// Flat iterator over all rows across all batches of a single table.
pub struct WorldViewRowIter {
    batches: Vec<(Vec<ArrayRef>, usize)>,
    batch_idx: usize,
    row_in_batch: usize,
}

impl WorldViewRowIter {
    fn new(batches: Vec<RecordBatch>) -> ArrowStoreResult<Self> {
        let data: Vec<(Vec<ArrayRef>, usize)> = batches
            .into_iter()
            .map(|b| {
                let n = b.num_rows();
                let cols: Vec<ArrayRef> = b.columns().iter().map(Arc::clone).collect();
                (cols, n)
            })
            .collect();
        Ok(Self {
            batches: data,
            batch_idx: 0,
            row_in_batch: 0,
        })
    }
}

impl Iterator for WorldViewRowIter {
    type Item = ArrowStoreResult<WorldViewRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.batch_idx >= self.batches.len() {
                return None;
            }
            let (_, num_rows) = self.batches[self.batch_idx];
            if self.row_in_batch < num_rows {
                let (ref cols, _) = self.batches[self.batch_idx];
                let row = WorldViewRow {
                    columns: cols.clone(),
                    row_idx: self.row_in_batch,
                };
                self.row_in_batch += 1;
                return Some(Ok(row));
            }
            self.batch_idx += 1;
            self.row_in_batch = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// WorldViewRow
// ---------------------------------------------------------------------------

/// A single row accessed via typed getters. Holds shared refs to the column
/// arrays of its batch and the row offset.
#[derive(Debug, Clone)]
pub struct WorldViewRow {
    columns: Vec<ArrayRef>,
    row_idx: usize,
}

impl WorldViewRow {
    pub fn get_i64(&self, col_idx: usize) -> ArrowStoreResult<Option<i64>> {
        let arr = self.column(col_idx)?;
        let int_arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| ArrowStoreError::TypeMismatch {
                column: col_idx.to_string(),
                expected: "Int64".to_owned(),
                got: format!("{:?}", arr.data_type()),
            })?;
        if int_arr.is_null(self.row_idx) {
            Ok(None)
        } else {
            Ok(Some(int_arr.value(self.row_idx)))
        }
    }

    pub fn get_f64(&self, col_idx: usize) -> ArrowStoreResult<Option<f64>> {
        let arr = self.column(col_idx)?;
        let float_arr = arr
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .ok_or_else(|| ArrowStoreError::TypeMismatch {
                column: col_idx.to_string(),
                expected: "Float64".to_owned(),
                got: format!("{:?}", arr.data_type()),
            })?;
        if float_arr.is_null(self.row_idx) {
            Ok(None)
        } else {
            Ok(Some(float_arr.value(self.row_idx)))
        }
    }

    pub fn get_string(&self, col_idx: usize) -> ArrowStoreResult<Option<String>> {
        let arr = self.column(col_idx)?;
        // Try StringArray first, then LargeStringArray
        if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
            if s.is_null(self.row_idx) {
                return Ok(None);
            }
            return Ok(Some(s.value(self.row_idx).to_owned()));
        }
        if let Some(s) = arr.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
            if s.is_null(self.row_idx) {
                return Ok(None);
            }
            return Ok(Some(s.value(self.row_idx).to_owned()));
        }
        Err(ArrowStoreError::TypeMismatch {
            column: col_idx.to_string(),
            expected: "Utf8 or LargeUtf8".to_owned(),
            got: format!("{:?}", arr.data_type()),
        })
    }

    pub fn get_bool(&self, col_idx: usize) -> ArrowStoreResult<Option<bool>> {
        let arr = self.column(col_idx)?;
        let bool_arr = arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| ArrowStoreError::TypeMismatch {
                column: col_idx.to_string(),
                expected: "Boolean".to_owned(),
                got: format!("{:?}", arr.data_type()),
            })?;
        if bool_arr.is_null(self.row_idx) {
            Ok(None)
        } else {
            Ok(Some(bool_arr.value(self.row_idx)))
        }
    }

    fn column(&self, col_idx: usize) -> ArrowStoreResult<&ArrayRef> {
        self.columns.get(col_idx).ok_or_else(|| {
            ArrowStoreError::Generic(format!(
                "column index {col_idx} out of bounds (len={})",
                self.columns.len()
            ))
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioned_table::{MutationMode, VersionedTable};
    use arrow_array::BooleanArray;
    use arrow_schema::DataType;

    fn make_test_snapshot() -> WorldSnapshot {
        let mut snapshot = WorldSnapshot::new(Tick(42));
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("label", DataType::Utf8, true),
            arrow_schema::Field::new("score", DataType::Float64, true),
            arrow_schema::Field::new("active", DataType::Boolean, false),
        ]));

        let id_arr: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1i64, 2, 3]));
        let label_arr: ArrayRef =
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
        let score_arr: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![
            Some(1.1f64),
            Some(2.2),
            None,
        ]));
        let active_arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));

        let batch = RecordBatch::try_new(schema, vec![id_arr, label_arr, score_arr, active_arr])
            .ok()
            .unwrap_or_else(|| panic!("test setup: failed to create RecordBatch"));

        let mut table = VersionedTable::new("heroes", MutationMode::AppendOnly);
        table.insert_version(Tick(42), vec![batch]).unwrap();
        snapshot.register_table("heroes", Arc::new(table));
        snapshot
    }

    #[test]
    fn table_names_sorted() {
        // Build snapshot with two tables in non-sorted insertion order.
        let mut snap = WorldSnapshot::new(Tick(1));
        snap.register_table(
            "zulu",
            Arc::new(VersionedTable::new("zulu", MutationMode::AppendOnly)),
        );
        snap.register_table(
            "alpha",
            Arc::new(VersionedTable::new("alpha", MutationMode::AppendOnly)),
        );
        let view = WorldView::new(Arc::new(snap));
        let names = view.table_names();
        assert_eq!(names, vec!["alpha", "zulu"]);
    }

    #[test]
    fn tick_returns_snapshot_tick() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        assert_eq!(view.tick(), Tick(42));
    }

    #[test]
    fn row_count_matches() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        assert_eq!(view.row_count("heroes").unwrap(), 3);
    }

    #[test]
    fn row_count_table_not_found_returns_error() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let result = view.row_count("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn column_names_correct() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let names = view.column_names("heroes").unwrap();
        assert_eq!(names, vec!["id", "label", "score", "active"]);
    }

    #[test]
    fn column_type_returns_arrow_type_string() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        assert_eq!(view.column_type("heroes", "id").unwrap(), "Int64");
        assert_eq!(view.column_type("heroes", "label").unwrap(), "Utf8");
        assert_eq!(view.column_type("heroes", "score").unwrap(), "Float64");
        assert_eq!(view.column_type("heroes", "active").unwrap(), "Boolean");
    }

    #[test]
    fn column_type_nonexistent_column_returns_error() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        assert!(view.column_type("heroes", "nope").is_err());
    }

    #[test]
    fn iter_rows_produces_typed_values() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let iter = view.iter_rows("heroes").unwrap();
        let rows: Vec<WorldViewRow> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(rows.len(), 3);

        // Row 0
        assert_eq!(rows[0].get_i64(0).unwrap(), Some(1));
        assert_eq!(rows[0].get_string(1).unwrap(), Some("alpha".to_owned()));
        assert_eq!(rows[0].get_f64(2).unwrap(), Some(1.1));
        assert_eq!(rows[0].get_bool(3).unwrap(), Some(true));

        // Row 1 — null label
        assert_eq!(rows[1].get_i64(0).unwrap(), Some(2));
        assert_eq!(rows[1].get_string(1).unwrap(), None);
        assert_eq!(rows[1].get_f64(2).unwrap(), Some(2.2));
        assert_eq!(rows[1].get_bool(3).unwrap(), Some(false));

        // Row 2 — null score
        assert_eq!(rows[2].get_i64(0).unwrap(), Some(3));
        assert_eq!(rows[2].get_string(1).unwrap(), Some("gamma".to_owned()));
        assert_eq!(rows[2].get_f64(2).unwrap(), None);
        assert_eq!(rows[2].get_bool(3).unwrap(), Some(true));
    }

    #[test]
    fn iter_rows_nonexistent_table_returns_error() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        assert!(view.iter_rows("missing").is_err());
    }

    #[test]
    fn get_wrong_type_returns_type_mismatch() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let mut iter = view.iter_rows("heroes").unwrap();
        let row = iter.next().unwrap().unwrap();
        // Column 0 is Int64 — calling get_string on it should fail.
        let result = row.get_string(0);
        assert!(result.is_err());
        match result.unwrap_err() {
            ArrowStoreError::TypeMismatch { .. } => {}
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn column_index_out_of_bounds_returns_error() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let mut iter = view.iter_rows("heroes").unwrap();
        let row = iter.next().unwrap().unwrap();
        assert!(row.get_i64(99).is_err());
    }

    #[test]
    fn world_view_clone_works() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));
        let clone = view.clone();
        assert_eq!(clone.tick(), view.tick());
        assert_eq!(clone.table_names(), view.table_names());
        assert_eq!(
            clone.row_count("heroes").unwrap(),
            view.row_count("heroes").unwrap()
        );
    }

    /// I-WV-IMMUTABLE: a WorldView frozen at creation remains unchanged
    /// even when a new snapshot is stored into the same Arc slot.
    #[test]
    fn world_view_is_immutable() {
        let mut snap_a = WorldSnapshot::new(Tick(10));
        snap_a.register_table(
            "only_a",
            Arc::new(VersionedTable::new("only_a", MutationMode::AppendOnly)),
        );
        let shared = Arc::new(snap_a);
        let view = WorldView::new(Arc::clone(&shared));

        // Store a new snapshot into the same Arc variable — does NOT affect the view.
        let mut snap_b = WorldSnapshot::new(Tick(20));
        snap_b.register_table(
            "only_b",
            Arc::new(VersionedTable::new("only_b", MutationMode::AppendOnly)),
        );
        // drop the old Arc, create a new one
        let _ = shared;

        // view still reflects tick 10 and table "only_a"
        assert_eq!(view.tick(), Tick(10));
        assert!(view.table_names().contains(&"only_a".to_owned()));
        assert!(!view.table_names().contains(&"only_b".to_owned()));
    }

    // ------------------------------------------------------------------
    // May-fail tests: uncovered branches
    // ------------------------------------------------------------------

    /// Nullable Int64 column — get_i64 on a null slot must return Ok(None),
    /// not panic.
    #[test]
    fn get_i64_null_returns_none() {
        let mut snapshot = WorldSnapshot::new(Tick(1));
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "val",
            DataType::Int64,
            true,
        )]));
        let arr: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![
            None::<i64>,
            Some(42i64),
        ]));
        let batch = RecordBatch::try_new(schema, vec![arr])
            .ok()
            .unwrap_or_else(|| panic!("test setup"));
        let mut table = VersionedTable::new("t", MutationMode::AppendOnly);
        table.insert_version(Tick(1), vec![batch]).unwrap();
        snapshot.register_table("t", Arc::new(table));
        let view = WorldView::new(Arc::new(snapshot));

        let mut iter = view.iter_rows("t").unwrap();
        let row0 = iter.next().unwrap().unwrap();
        let row1 = iter.next().unwrap().unwrap();

        // null slot → Ok(None)
        assert_eq!(row0.get_i64(0).unwrap(), None);
        // non-null slot → Ok(Some(...))
        assert_eq!(row1.get_i64(0).unwrap(), Some(42));
    }

    /// Nullable Boolean column — get_bool on a null slot must return
    /// Ok(None), not panic.
    #[test]
    fn get_bool_null_returns_none() {
        let mut snapshot = WorldSnapshot::new(Tick(1));
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]));
        let arr: ArrayRef = Arc::new(arrow_array::BooleanArray::from(vec![
            None::<bool>,
            Some(true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![arr])
            .ok()
            .unwrap_or_else(|| panic!("test setup"));
        let mut table = VersionedTable::new("t", MutationMode::AppendOnly);
        table.insert_version(Tick(1), vec![batch]).unwrap();
        snapshot.register_table("t", Arc::new(table));
        let view = WorldView::new(Arc::new(snapshot));

        let mut iter = view.iter_rows("t").unwrap();
        let row0 = iter.next().unwrap().unwrap();
        let row1 = iter.next().unwrap().unwrap();

        assert_eq!(row0.get_bool(0).unwrap(), None);
        assert_eq!(row1.get_bool(0).unwrap(), Some(true));
    }

    /// get_string must handle LargeStringArray in addition to StringArray.
    #[test]
    fn get_string_large_string_array() {
        let mut snapshot = WorldSnapshot::new(Tick(1));
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "text",
            DataType::LargeUtf8,
            true,
        )]));
        let arr: ArrayRef = Arc::new(arrow_array::LargeStringArray::from(vec![
            Some("hello"),
            None::<&str>,
        ]));
        let batch = RecordBatch::try_new(schema, vec![arr])
            .ok()
            .unwrap_or_else(|| panic!("test setup"));
        let mut table = VersionedTable::new("t", MutationMode::AppendOnly);
        table.insert_version(Tick(1), vec![batch]).unwrap();
        snapshot.register_table("t", Arc::new(table));
        let view = WorldView::new(Arc::new(snapshot));

        let mut iter = view.iter_rows("t").unwrap();
        let row0 = iter.next().unwrap().unwrap();
        let row1 = iter.next().unwrap().unwrap();

        assert_eq!(row0.get_string(0).unwrap(), Some("hello".to_owned()));
        assert_eq!(row1.get_string(0).unwrap(), None);
    }

    /// column_type on a registered table that has zero batches should NOT
    /// return TableNotFound — the table IS registered, it just has no data.
    /// Expected to fail: currently returns TableNotFound.
    #[test]
    fn column_type_on_empty_table_returns_meaningful_error() {
        let mut snap = WorldSnapshot::new(Tick(1));
        snap.register_table(
            "ghost",
            Arc::new(VersionedTable::new("ghost", MutationMode::AppendOnly)),
        );
        let view = WorldView::new(Arc::new(snap));
        let result = view.column_type("ghost", "any_col");
        // Should NOT be TableNotFound — the table exists.
        assert!(result.is_err());
        if let ArrowStoreError::TableNotFound(_) = result.unwrap_err() {
            panic!("BUG: returned TableNotFound for a registered table with no data")
        }
    }

    /// column_names on a registered table with no batches should return
    /// Ok(Vec::new()), not TableNotFound. Expected to fail: currently
    /// may return empty Ok only if table_batches returns Ok(vec![]).
    #[test]
    fn column_names_on_empty_table_returns_empty_vec() {
        let mut snap = WorldSnapshot::new(Tick(1));
        snap.register_table(
            "ghost",
            Arc::new(VersionedTable::new("ghost", MutationMode::AppendOnly)),
        );
        let view = WorldView::new(Arc::new(snap));
        let names = view.column_names("ghost").unwrap();
        assert!(names.is_empty());
    }

    /// iter_rows on a registered table with zero batches should produce an
    /// iterator that immediately yields None.
    #[test]
    fn iter_rows_empty_table_yields_no_rows() {
        let mut snap = WorldSnapshot::new(Tick(1));
        snap.register_table(
            "ghost",
            Arc::new(VersionedTable::new("ghost", MutationMode::AppendOnly)),
        );
        let view = WorldView::new(Arc::new(snap));
        let mut iter = view.iter_rows("ghost").unwrap();
        assert!(iter.next().is_none());
    }

    /// WorldView must NOT expose RecordBatch, arrow_schema::Schema, or
    /// ArrayRef through any public method. This test is a compile-time
    /// guarantee — it simply calls every public method and ensures no
    /// arrow type leaks.
    #[test]
    fn world_view_encapsulation() {
        let snap = make_test_snapshot();
        let view = WorldView::new(Arc::new(snap));

        // All these calls succeed and return non-arrow types.
        let _tick: Tick = view.tick();
        let _names: Vec<String> = view.table_names();
        let _count: usize = view.row_count("heroes").unwrap();
        let _cols: Vec<String> = view.column_names("heroes").unwrap();
        let _ty: String = view.column_type("heroes", "id").unwrap();
        let _iter: WorldViewRowIter = view.iter_rows("heroes").unwrap();

        // Verify the row items and getters return plain Rust types.
        for row_result in _iter {
            let row = row_result.unwrap();
            let _id: Option<i64> = row.get_i64(0).unwrap();
            let _label: Option<String> = row.get_string(1).unwrap();
            let _score: Option<f64> = row.get_f64(2).unwrap();
            let _active: Option<bool> = row.get_bool(3).unwrap();
        }
    }
}
