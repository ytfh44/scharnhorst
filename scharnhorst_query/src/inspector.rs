use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use scharnhorst_schema::TableSpec;

use crate::error::{QueryError, QueryResult};
use crate::typed_access::ColumnView;

/// A lightweight row representation for inspector display.
#[derive(Debug, Clone, PartialEq)]
pub struct InspectorRow {
    pub values: Vec<String>,
}

impl InspectorRow {
    pub fn new(values: Vec<String>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, idx: usize) -> Option<&str> {
        self.values.get(idx).map(|s| s.as_str())
    }
}

/// A paginated page of inspector data for a single table.
#[derive(Debug, Clone, PartialEq)]
pub struct InspectorPage {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub rows: Vec<InspectorRow>,
    pub page_index: usize,
    pub page_size: usize,
    pub total_rows: usize,
}

impl InspectorPage {
    pub fn new(
        table_name: impl Into<String>,
        column_names: Vec<String>,
        rows: Vec<InspectorRow>,
        page_index: usize,
        page_size: usize,
        total_rows: usize,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            column_names,
            rows,
            page_index,
            page_size,
            total_rows,
        }
    }

    pub fn has_next(&self) -> bool {
        (self.page_index + 1) * self.page_size < self.total_rows
    }

    pub fn has_prev(&self) -> bool {
        self.page_index > 0
    }
}

/// Summary statistics for a single column, used by the inspector.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSummary {
    pub name: String,
    pub data_type: String,
    pub null_count: usize,
    pub value_count: usize,
}

impl ColumnSummary {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            null_count: 0,
            value_count: 0,
        }
    }

    pub fn with_counts(mut self, null_count: usize, value_count: usize) -> Self {
        self.null_count = null_count;
        self.value_count = value_count;
        self
    }
}

/// Summary information for a table, suitable for a console overview.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSummary {
    pub name: String,
    pub row_count: usize,
    pub column_summaries: Vec<ColumnSummary>,
}

impl TableSummary {
    pub fn new(name: impl Into<String>, row_count: usize) -> Self {
        Self {
            name: name.into(),
            row_count,
            column_summaries: Vec::new(),
        }
    }

    pub fn with_columns(mut self, columns: Vec<ColumnSummary>) -> Self {
        self.column_summaries = columns;
        self
    }
}

/// A console-like interface for browsing tables and their contents.
///
/// This is intentionally simple: it operates on `RecordBatch` data and
/// returns plain Rust types so that it can be wired to any UI backend.
#[derive(Debug, Clone)]
pub struct InspectorConsole {
    schemas: HashMap<String, Arc<TableSpec>>,
}

impl InspectorConsole {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    pub fn register_schema(&mut self, schema: Arc<TableSpec>) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    pub fn unregister_schema(&mut self, table_name: &str) {
        self.schemas.remove(table_name);
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.schemas.keys().map(|s| s.as_str())
    }

    pub fn schema(&self, table_name: &str) -> QueryResult<&TableSpec> {
        self.schemas
            .get(table_name)
            .map(|arc| arc.as_ref())
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))
    }

 /// Summarise a table from a set of record batches.
    pub fn summarize_table(&self, table_name: &str, batches: &[RecordBatch]) -> QueryResult<TableSummary> {
        let schema = self.schema(table_name)?;
        let row_count = batches.iter().map(|b| b.num_rows()).sum();

        let column_summaries = schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col_spec)| {
                let (null_count, value_count) = batches
                    .iter()
                    .map(|b| {
                        let col = b.column(idx);
                        (col.null_count(), col.len() - col.null_count())
                    })
                    .fold((0, 0), |(na, va), (nb, vb)| (na + nb, va + vb));

                ColumnSummary::new(&col_spec.name, &col_spec.storage_type)
                    .with_counts(null_count, value_count)
            })
            .collect();

        Ok(TableSummary::new(table_name, row_count).with_columns(column_summaries))
    }

 /// Extract a paginated page of stringified rows from record batches.
    pub fn inspect_page(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
        page_index: usize,
        page_size: usize,
    ) -> QueryResult<InspectorPage> {
        let schema = self.schema(table_name)?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let start = page_index.saturating_mul(page_size);
        if start > total_rows {
            return Err(QueryError::IndexOutOfBounds(start));
        }
        let end = (start + page_size).min(total_rows);

        let column_names: Vec<String> = schema
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();

        let mut rows: Vec<InspectorRow> = Vec::with_capacity(end - start);
        let mut global_row = 0usize;

        for batch in batches {
            let batch_rows = batch.num_rows();
            let batch_start = global_row;
            let batch_end = global_row + batch_rows;

            let overlap_start = start.max(batch_start);
            let overlap_end = end.min(batch_end);

            if overlap_start < overlap_end {
                let local_start = overlap_start - batch_start;
                let local_end = overlap_end - batch_start;

                for r in local_start..local_end {
                    let values: Vec<String> = (0..batch.num_columns())
                        .map(|c| format_cell(batch.column(c), r))
                        .collect();
                    rows.push(InspectorRow::new(values));
                }
            }

            global_row += batch_rows;
            if global_row >= end {
                break;
            }
        }

        Ok(InspectorPage::new(
            table_name,
            column_names,
            rows,
            page_index,
            page_size,
            total_rows,
        ))
    }

 /// Build a map of column views for the first batch of a table.
    pub fn column_views(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> QueryResult<HashMap<String, ColumnView>> {
        let _schema = self.schema(table_name)?;
        Ok(batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let view = ColumnView::new(batch.column(idx).clone(), field.name());
                (field.name().clone(), view)
            })
            .collect())
    }
}

impl Default for InspectorConsole {
    fn default() -> Self {
        Self::new()
    }
}

fn format_cell(array: &Arc<dyn arrow_array::Array>, row: usize) -> String {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    if array.is_null(row) {
        return "NULL".to_owned();
    }

    match array.data_type() {
        DataType::Int64 => array.as_primitive::<arrow_array::types::Int64Type>().value(row).to_string(),
        DataType::Float64 => array.as_primitive::<arrow_array::types::Float64Type>().value(row).to_string(),
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::Utf8 => array.as_string::<i32>().value(row).to_owned(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_owned(),
        other => format!("{:?}", other),
    }
}
