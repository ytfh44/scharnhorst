use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use arrow_schema::DataType;
use scharnhorst_core::{RowId, RowLookup};

use crate::error::{QueryError, QueryResult};
use crate::unified_read::TableReadView;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    Int64,
    UInt64,
    Float64,
    Boolean,
    Utf8,
    LargeUtf8,
    Null,
    Unknown,
}

/// Trait for reading a strongly-typed value from an Arrow array at a given row.
pub trait TypedColumnAccess<T> {
    /// Returns the value at `row` if it exists and is valid, otherwise an error.
    fn get(&self, row: usize) -> QueryResult<Option<T>>;

    /// Returns the number of rows in the column.
    fn len(&self) -> usize;

    /// Returns true if the column has no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A view over a single column within a table snapshot, providing typed access.
#[derive(Debug, Clone)]
pub struct ColumnView {
    array: ArrayRef,
    field_name: String,
}

impl ColumnView {
    pub fn new(array: ArrayRef, field_name: impl Into<String>) -> Self {
        Self {
            array,
            field_name: field_name.into(),
        }
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn data_type(&self) -> &DataType {
        self.array.data_type()
    }

    pub fn kind(&self) -> ColumnKind {
        match self.array.data_type() {
            DataType::Int64 => ColumnKind::Int64,
            DataType::UInt64 => ColumnKind::UInt64,
            DataType::Float64 => ColumnKind::Float64,
            DataType::Boolean => ColumnKind::Boolean,
            DataType::Utf8 => ColumnKind::Utf8,
            DataType::LargeUtf8 => ColumnKind::LargeUtf8,
            DataType::Null => ColumnKind::Null,
            _ => ColumnKind::Unknown,
        }
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    /// Downcast to a specific Arrow array type and return a reference.
    pub fn as_typed_array<A: Array + 'static>(&self) -> QueryResult<&A> {
        self.array
            .as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| QueryError::TypeMismatch {
                column: self.field_name.clone(),
                expected: std::any::type_name::<A>().to_owned(),
                got: format!("{:?}", self.array.data_type()),
            })
    }
}

macro_rules! impl_typed_access_primitive {
    ($rust_type:ty, $array_type:ty) => {
        impl TypedColumnAccess<$rust_type> for ColumnView {
            fn get(&self, row: usize) -> QueryResult<Option<$rust_type>> {
                let arr = self.as_typed_array::<$array_type>()?;
                if row >= arr.len() {
                    return Err(QueryError::IndexOutOfBounds(row));
                }
                Ok(if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                })
            }

            fn len(&self) -> usize {
                self.array.len()
            }
        }
    };
}

impl_typed_access_primitive!(i64, Int64Array);
impl_typed_access_primitive!(f64, Float64Array);
impl_typed_access_primitive!(u64, UInt64Array);
impl_typed_access_primitive!(bool, BooleanArray);

impl TypedColumnAccess<String> for ColumnView {
    fn get(&self, row: usize) -> QueryResult<Option<String>> {
        let arr = self.as_typed_array::<StringArray>()?;
        if row >= arr.len() {
            return Err(QueryError::IndexOutOfBounds(row));
        }
        Ok(if arr.is_null(row) {
            None
        } else {
            Some(arr.value(row).to_owned())
        })
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}

/// A row-oriented cursor over a set of column views, allowing typed field access by name.
#[derive(Debug, Clone)]
pub struct RowCursor {
    columns: Vec<(String, ColumnView)>,
    row_count: usize,
    current_row: usize,
}

impl RowCursor {
    pub fn new(columns: Vec<(String, ColumnView)>) -> QueryResult<Self> {
        let row_count = columns.first().map(|(_, view)| view.len()).unwrap_or(0);

        let mismatched = columns
            .iter()
            .find(|(_, view)| view.len() != row_count)
            .map(|(name, view)| (name.clone(), view.len()));

        if let Some((name, len)) = mismatched {
            return Err(QueryError::InvalidQuery(format!(
                "column '{}' has {} rows, expected {}",
                name, len, row_count
            )));
        }

        Ok(Self {
            columns,
            row_count,
            current_row: 0,
        })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn current_row(&self) -> usize {
        self.current_row
    }

    pub fn advance(&mut self) -> bool {
        if self.current_row + 1 < self.row_count {
            self.current_row += 1;
            true
        } else {
            false
        }
    }

    pub fn reset(&mut self) {
        self.current_row = 0;
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.columns.iter().map(|(name, _)| name.as_str())
    }

    pub fn get_column(&self, name: &str) -> QueryResult<&ColumnView> {
        self.columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, view)| view)
            .ok_or_else(|| QueryError::ColumnNotFound {
                column: name.to_owned(),
                table: String::new(),
            })
    }

    pub fn get_typed<T>(&self, name: &str) -> QueryResult<Option<T>>
    where
        ColumnView: TypedColumnAccess<T>,
    {
        let col = self.get_column(name)?;
        col.get(self.current_row)
    }

    pub fn get_row_id(&self, id_column: &str) -> QueryResult<Option<RowId>> {
        self.get_typed::<i64>(id_column)
            .map(|opt| opt.and_then(|v| v.try_into().ok()).map(RowId::new))
    }
}

/// A batched columnar reader that yields typed slices over contiguous row ranges.
#[derive(Debug, Clone)]
pub struct BatchColumnReader {
    arrays: Vec<(String, ArrayRef)>,
    row_count: usize,
}

impl BatchColumnReader {
    pub fn new(arrays: Vec<(String, ArrayRef)>) -> QueryResult<Self> {
        let row_count = arrays.first().map(|(_, arr)| arr.len()).unwrap_or(0);

        let mismatched = arrays
            .iter()
            .find(|(_, arr)| arr.len() != row_count)
            .map(|(name, arr)| (name.clone(), arr.len()));

        if let Some((name, len)) = mismatched {
            return Err(QueryError::InvalidQuery(format!(
                "column '{}' has {} rows, expected {}",
                name, len, row_count
            )));
        }

        Ok(Self { arrays, row_count })
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.arrays.iter().map(|(name, _)| name.as_str())
    }

    pub fn array_ref(&self, name: &str) -> QueryResult<&ArrayRef> {
        self.arrays
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, arr)| arr)
            .ok_or_else(|| QueryError::ColumnNotFound {
                column: name.to_owned(),
                table: String::new(),
            })
    }

    pub fn typed_array<A: Array + 'static>(&self, name: &str) -> QueryResult<&A> {
        let arr = self.array_ref(name)?;
        arr.as_any()
            .downcast_ref::<A>()
            .ok_or_else(|| QueryError::TypeMismatch {
                column: name.to_owned(),
                expected: std::any::type_name::<A>().to_owned(),
                got: format!("{:?}", arr.data_type()),
            })
    }

    pub fn bool_slice(&self, name: &str) -> QueryResult<BooleanSlice<'_>> {
        let arr = self.typed_array::<BooleanArray>(name)?;
        Ok(BooleanSlice { array: arr })
    }

    pub fn i64_slice(&self, name: &str) -> QueryResult<I64Slice<'_>> {
        let arr = self.typed_array::<Int64Array>(name)?;
        Ok(I64Slice { array: arr })
    }

    pub fn f64_slice(&self, name: &str) -> QueryResult<F64Slice<'_>> {
        let arr = self.typed_array::<Float64Array>(name)?;
        Ok(F64Slice { array: arr })
    }

    pub fn string_slice(&self, name: &str) -> QueryResult<StringSlice<'_>> {
        let arr = self.typed_array::<StringArray>(name)?;
        Ok(StringSlice { array: arr })
    }
}

/// Read-only view over a `BooleanArray`.
#[derive(Debug, Clone)]
pub struct BooleanSlice<'a> {
    array: &'a BooleanArray,
}

impl BooleanSlice<'_> {
    pub fn get(&self, row: usize) -> QueryResult<Option<bool>> {
        if row >= self.array.len() {
            return Err(QueryError::IndexOutOfBounds(row));
        }
        Ok(if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row))
        })
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    pub fn iter_valid(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.array.len())
            .filter(move |&i| self.array.is_valid(i))
            .map(move |i| self.array.value(i))
    }
}

/// Read-only view over an `Int64Array`.
#[derive(Debug, Clone)]
pub struct I64Slice<'a> {
    array: &'a Int64Array,
}

impl I64Slice<'_> {
    pub fn get(&self, row: usize) -> QueryResult<Option<i64>> {
        if row >= self.array.len() {
            return Err(QueryError::IndexOutOfBounds(row));
        }
        Ok(if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row))
        })
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    pub fn iter_valid(&self) -> impl Iterator<Item = i64> + '_ {
        (0..self.array.len())
            .filter(move |&i| self.array.is_valid(i))
            .map(move |i| self.array.value(i))
    }
}

/// Read-only view over a `Float64Array`.
#[derive(Debug, Clone)]
pub struct F64Slice<'a> {
    array: &'a Float64Array,
}

impl F64Slice<'_> {
    pub fn get(&self, row: usize) -> QueryResult<Option<f64>> {
        if row >= self.array.len() {
            return Err(QueryError::IndexOutOfBounds(row));
        }
        Ok(if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row))
        })
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    pub fn iter_valid(&self) -> impl Iterator<Item = f64> + '_ {
        (0..self.array.len())
            .filter(move |&i| self.array.is_valid(i))
            .map(move |i| self.array.value(i))
    }
}

/// Read-only view over a `StringArray`.
#[derive(Debug, Clone)]
pub struct StringSlice<'a> {
    array: &'a StringArray,
}

impl StringSlice<'_> {
    pub fn get(&self, row: usize) -> QueryResult<Option<String>> {
        if row >= self.array.len() {
            return Err(QueryError::IndexOutOfBounds(row));
        }
        Ok(if self.array.is_null(row) {
            None
        } else {
            Some(self.array.value(row).to_owned())
        })
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }

    pub fn iter_valid(&self) -> impl Iterator<Item = String> + '_ {
        (0..self.array.len())
            .filter(move |&i| self.array.is_valid(i))
            .map(move |i| self.array.value(i).to_owned())
    }
}

/// A view over a single row resolved via `lookup_row`.
///
/// Provides typed column access without exposing raw array indices.
/// The underlying `TableReadView` is owned (cloned from cache) to avoid
/// lifetime coupling with the query engine's internal locks.
#[derive(Debug, Clone)]
pub struct RowLookupView {
    lookup: RowLookup,
    view: TableReadView,
}

impl RowLookupView {
    pub fn new(lookup: RowLookup, view: TableReadView) -> Self {
        Self { lookup, view }
    }

    pub fn row_id(&self) -> RowId {
        self.lookup.row_id()
    }

    /// Resolve a column view for the target batch and column name.
    fn column_view(&self, column: &str) -> QueryResult<ColumnView> {
        let batch_idx = self.lookup.batch_index();
        let batch = self.view.batches().nth(batch_idx).ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "batch index {} out of range for table '{}'",
                batch_idx,
                self.view.table_name()
            ))
        })?;
        let col_idx = batch
            .schema()
            .index_of(column)
            .map_err(|_| QueryError::ColumnNotFound {
                column: column.to_owned(),
                table: self.view.table_name().to_owned(),
            })?;
        let array = batch.column(col_idx).clone();
        Ok(ColumnView::new(array, column))
    }

    pub fn get_i64(&self, column: &str) -> QueryResult<Option<i64>> {
        let cv = self.column_view(column)?;
        cv.get(self.lookup.row_offset())
    }

    pub fn get_u64(&self, column: &str) -> QueryResult<Option<u64>> {
        let cv = self.column_view(column)?;
        cv.get(self.lookup.row_offset())
    }

    pub fn get_f64(&self, column: &str) -> QueryResult<Option<f64>> {
        let cv = self.column_view(column)?;
        cv.get(self.lookup.row_offset())
    }

    pub fn get_bool(&self, column: &str) -> QueryResult<Option<bool>> {
        let cv = self.column_view(column)?;
        cv.get(self.lookup.row_offset())
    }

    pub fn get_string(&self, column: &str) -> QueryResult<Option<String>> {
        let cv = self.column_view(column)?;
        cv.get(self.lookup.row_offset())
    }
}
