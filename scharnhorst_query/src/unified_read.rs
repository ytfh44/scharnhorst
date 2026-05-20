use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use scharnhorst_core::{RowId, RowPositionMap, Tick};
use scharnhorst_schema::{ColumnSpec, TableSpec};

use crate::error::{QueryError, QueryResult};
use crate::typed_access::{BatchColumnReader, ColumnView, RowCursor};

/// The canonical read-only view of a single table at a specific tick.
///
/// All consumers (game logic, UI, AI, diagnostics) must route through this
/// structure so that the query engine can enforce caching, access control,
/// and semantic awareness in one place.
#[derive(Debug, Clone)]
pub struct TableReadView {
    table_name: String,
    tick: Tick,
    batches: Vec<RecordBatch>,
    schema: Arc<TableSpec>,
    position_map: RowPositionMap,
}

impl TableReadView {
    pub fn new(
        table_name: impl Into<String>,
        tick: Tick,
        batches: Vec<RecordBatch>,
        schema: Arc<TableSpec>,
        position_map: Option<RowPositionMap>,
    ) -> Self {
        let table_name = table_name.into();
        let batches = if batches.len() > 1 {
            let merged_schema = batches[0].schema();
            match concat_batches(&merged_schema, &batches) {
                Ok(merged) => vec![merged],
                Err(e) => {
                    tracing::warn!("concat_batches failed for table '{}': {}", table_name, e);
                    batches
                }
            }
        } else {
            batches
        };

        let position_map = position_map.unwrap_or_else(|| Self::build_position_map(&batches));

        Self {
            table_name,
            tick,
            batches,
            schema,
            position_map,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn tick(&self) -> Tick {
        self.tick
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    pub fn schema(&self) -> &TableSpec {
        &self.schema
    }

    pub fn column_spec(&self, name: &str) -> QueryResult<&ColumnSpec> {
        self.schema
            .column_by_name(name)
            .ok_or_else(|| QueryError::ColumnNotFound {
                column: name.to_owned(),
                table: self.table_name.clone(),
            })
    }

    /// Returns an iterator over all record batches.
    pub fn batches(&self) -> impl Iterator<Item = &RecordBatch> + '_ {
        self.batches.iter()
    }

    /// Returns a `BatchColumnReader` for the first batch, if any.
    pub fn first_batch_reader(&self) -> QueryResult<BatchColumnReader> {
        let batch = self.batches.first().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "table '{}' has no batches at tick {}",
                self.table_name, self.tick
            ))
        })?;

        let arrays = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().clone(), batch.column(idx).clone()))
            .collect();

        BatchColumnReader::new(arrays)
    }

    /// Returns a `RowCursor` over the first batch, if any.
    pub fn first_batch_cursor(&self) -> QueryResult<RowCursor> {
        let batch = self.batches.first().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "table '{}' has no batches at tick {}",
                self.table_name, self.tick
            ))
        })?;

        let columns: Vec<(String, ColumnView)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let view = ColumnView::new(batch.column(idx).clone(), field.name());
                (field.name().clone(), view)
            })
            .collect();

        RowCursor::new(columns)
    }

    /// Returns a map of column name -> `ColumnView` for the first batch.
    pub fn first_batch_columns(&self) -> QueryResult<HashMap<String, ColumnView>> {
        let batch = self.batches.first().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "table '{}' has no batches at tick {}",
                self.table_name, self.tick
            ))
        })?;

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

    /// Returns a reference to the position map for looking up RowIds.
    pub fn position_map(&self) -> &RowPositionMap {
        &self.position_map
    }

    /// Build a `RowPositionMap` from a slice of record batches.
    ///
    /// Each row is assigned a sequential `RowId` starting from 0.
    fn build_position_map(batches: &[RecordBatch]) -> RowPositionMap {
        let mut map = RowPositionMap::new();
        let mut global_row: u64 = 0;
        for (batch_idx, batch) in batches.iter().enumerate() {
            for offset in 0..batch.num_rows() {
                map.insert(RowId::new(global_row), batch_idx, offset);
                global_row += 1;
            }
        }
        map
    }
}

/// A request to read one or more tables at a specific tick.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub tick: Tick,
    pub table_names: Vec<String>,
}

impl ReadRequest {
    pub fn new(tick: Tick) -> Self {
        Self {
            tick,
            table_names: Vec::new(),
        }
    }

    pub fn with_table(mut self, name: impl Into<String>) -> Self {
        self.table_names.push(name.into());
        self
    }

    pub fn with_tables(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.table_names.extend(names.into_iter().map(Into::into));
        self
    }
}

/// The response to a `ReadRequest`, containing read-only views for each table.
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub tick: Tick,
    pub views: HashMap<String, TableReadView>,
}

impl ReadResponse {
    pub fn new(tick: Tick) -> Self {
        Self {
            tick,
            views: HashMap::new(),
        }
    }

    pub fn insert(&mut self, view: TableReadView) {
        self.views.insert(view.table_name().to_owned(), view);
    }

    pub fn get(&self, table_name: &str) -> QueryResult<&TableReadView> {
        self.views
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> + '_ {
        self.views.keys().map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }
}

/// Trait for types that can fulfill a `ReadRequest` and produce a `ReadResponse`.
///
/// This is the single abstraction that all read-only consumers depend on.
pub trait UnifiedReadSource: Send + Sync {
    fn read(&self, request: ReadRequest) -> QueryResult<ReadResponse>;
}

/// A simple in-memory `UnifiedReadSource` backed by a map of table views.
#[derive(Debug, Clone, Default)]
pub struct InMemoryReadSource {
    views: HashMap<String, TableReadView>,
}

impl InMemoryReadSource {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, view: TableReadView) {
        self.views.insert(view.table_name().to_owned(), view);
    }

    pub fn unregister(&mut self, table_name: &str) -> QueryResult<()> {
        self.views
            .remove(table_name)
            .map(|_| ())
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))
    }

    pub fn clear(&mut self) {
        self.views.clear();
    }
}

impl UnifiedReadSource for InMemoryReadSource {
    fn read(&self, request: ReadRequest) -> QueryResult<ReadResponse> {
        let mut response = ReadResponse::new(request.tick);

        for name in &request.table_names {
            let view = self
                .views
                .get(name)
                .ok_or_else(|| QueryError::TableNotFound(name.clone()))?;
            response.insert(view.clone());
        }

        Ok(response)
    }
}
