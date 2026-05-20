use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

use arrow_array::RecordBatch;
use scharnhorst_arrow_store::SnapshotIngestor;
use scharnhorst_arrow_store::WorldSnapshot;
use scharnhorst_core::{Diff, JournalSubmitToken, RowId, RowLookup, RowPositionMap, Tick};
use scharnhorst_schema::{RelationEdge, SchemaRegistry, TableSpec};

use crate::debug_write::{DebugWriteJournal, DebugWriteOp};
use crate::error::{QueryError, QueryResult};
use crate::inspector::{InspectorConsole, InspectorPage, TableSummary};
use crate::sql_interface::SqlExecutionContext;
use crate::typed_access::{BatchColumnReader, ColumnView, RowCursor, RowLookupView};
use crate::unified_read::{ReadRequest, ReadResponse, TableReadView, UnifiedReadSource};

/// The central query engine that all read-only consumers must route through.
///
/// Responsibilities:
/// - Cache schema-registry metadata for semantic-aware queries.
/// - Provide typed columnar access over Arrow data.
/// - Expose a unified read interface (`UnifiedReadSource`).
/// - Integrate DataFusion for SQL-based analysis.
/// - Host the `DebugWriteJournal` (debug builds only).
/// - Host the `InspectorConsole` for developer tooling.
#[derive(Debug, Clone)]
pub struct QueryEngine {
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    sql_context: Arc<RwLock<SqlExecutionContext>>,
    inspector: Arc<RwLock<InspectorConsole>>,
    debug_journal: Arc<DebugWriteJournal>,
    /// In-memory cache of the latest tick for which we have snapshot data.
    /// INVARIANT: u64::MAX is reserved as the None sentinel.
    latest_tick: Arc<AtomicU64>,
    /// Cached table views keyed by table name (populated on read).
    view_cache: Arc<RwLock<HashMap<String, TableReadView>>>,
    /// Cached reference to the latest WorldSnapshot, pushed via store_world_snapshot()
    /// during journal.commit(). Consumers obtain this via snapshot().
    latest_snapshot: Arc<RwLock<Option<Arc<WorldSnapshot>>>>,
}

impl QueryEngine {
    pub fn new(schema_registry: SchemaRegistry) -> Self {
        let inspector = InspectorConsole::new();
        Self {
            schema_registry: Arc::new(RwLock::new(schema_registry)),
            sql_context: Arc::new(RwLock::new(SqlExecutionContext::new())),
            inspector: Arc::new(RwLock::new(inspector)),
            debug_journal: Arc::new(DebugWriteJournal::default()),
            latest_tick: Arc::new(AtomicU64::new(u64::MAX)),
            view_cache: Arc::new(RwLock::new(HashMap::new())),
            latest_snapshot: Arc::new(RwLock::new(None)),
        }
    }

    // ------------------------------------------------------------------
    // Schema registry access
    // ------------------------------------------------------------------

    pub fn schema_registry(
        &self,
    ) -> QueryResult<impl std::ops::Deref<Target = SchemaRegistry> + '_> {
        self.schema_registry
            .read()
            .map_err(|_| QueryError::SchemaRegistry("poisoned lock".to_owned()))
    }

    pub(crate) fn schema_registry_mut(
        &self,
    ) -> QueryResult<impl std::ops::DerefMut<Target = SchemaRegistry> + '_> {
        self.schema_registry
            .write()
            .map_err(|_| QueryError::SchemaRegistry("poisoned lock".to_owned()))
    }

    pub fn register_table_schema(&self, spec: TableSpec) -> QueryResult<()> {
        let mut reg = self.schema_registry_mut()?;
        reg.register(spec.clone())?;
        let mut insp = self
            .inspector
            .write()
            .map_err(|_| QueryError::Inspector("poisoned lock".to_owned()))?;
        insp.register_schema(Arc::new(spec));
        Ok(())
    }

    pub fn table_schema(&self, name: &str) -> QueryResult<TableSpec> {
        let reg = self.schema_registry()?;
        reg.get(name).cloned().map_err(|e| e.into())
    }

    /// Resolve a relation edge by key (format: "{from} -> {to}").
    ///
    /// This is the exclusive access path to the [`RelationGraph`] for all
    /// consumers. The underlying relation graph is owned by the
    /// schema-registry; no consumer may query it directly.
    pub fn resolve_relation_edge(&self, key: &str) -> QueryResult<RelationEdge> {
        let reg = self.schema_registry()?;
        let graph = reg.relation_graph();
        graph
            .all_edges()
            .iter()
            .find(|e| {
                let edge_key = format!("{} -> {}", e.from, e.to);
                edge_key == key
            })
            .cloned()
            .ok_or_else(|| QueryError::RelationNotFound(key.to_owned()))
    }

    // ------------------------------------------------------------------
    // Unified read interface
    // ------------------------------------------------------------------

    pub fn read(&self, request: ReadRequest) -> QueryResult<ReadResponse> {
        let cache_tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;

        if request.tick != Tick::ZERO && cache_tick != Some(request.tick) {
            return Err(QueryError::TicksMismatch {
                requested: request.tick,
                actual: cache_tick,
            });
        }

        let mut response = ReadResponse::new(request.tick);
        for name in &request.table_names {
            let view = cache
                .get(name)
                .cloned()
                .ok_or_else(|| QueryError::TableNotFound(name.clone()))?;
            response.insert(view);
        }
        Ok(response)
    }

    pub fn read_single_table(&self, tick: Tick, table_name: &str) -> QueryResult<TableReadView> {
        let req = ReadRequest::new(tick).with_table(table_name);
        let resp = self.read(req)?;
        resp.get(table_name).cloned()
    }

    // ------------------------------------------------------------------
    // Snapshot ingestion (bridging from arrow_store)
    // ------------------------------------------------------------------

    /// ORDERING INVARIANT: Readers MUST load `latest_tick()` BEFORE
    /// acquiring the `view_cache` read lock. Violating this order may
    /// cause stale cache data to be attributed to a newer tick.
    ///
    /// SENTINEL: `u64::MAX` is reserved. `tick.as_u64()` must never equal
    /// `u64::MAX` (see [`Tick::MAX`]).
    pub fn ingest_snapshot(
        &self,
        tick: Tick,
        table_name: &str,
        batches: Vec<RecordBatch>,
        position_map: RowPositionMap,
    ) -> QueryResult<()> {
        if tick.as_u64() == u64::MAX {
            return Err(QueryError::UnsupportedOperation(
                "Tick sentinel collision: u64::MAX is reserved".to_owned(),
            ));
        }

        let schema = self.table_schema(table_name)?;
        let view = TableReadView::new(
            table_name,
            tick,
            batches,
            Arc::new(schema),
            Some(position_map),
        );

        let mut cache = self
            .view_cache
            .write()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        cache.insert(table_name.to_owned(), view);

        self.latest_tick.store(tick.as_u64(), Ordering::Release);

        Ok(())
    }

    /// Returns the latest tick for which snapshot data is available.
    ///
    /// NOTE: The (Release) store in [`ingest_snapshot`] pairs with this
    /// (Acquire) load. To maintain the ordering invariant, call this
    /// method BEFORE reading `view_cache`.
    ///
    /// INVARIANT: `u64::MAX` is reserved as the `None` sentinel.
    /// Returns `None` when no snapshot has been ingested yet.
    pub fn latest_tick(&self) -> QueryResult<Option<Tick>> {
        let raw = self.latest_tick.load(Ordering::Acquire);
        if raw == u64::MAX {
            Ok(None)
        } else {
            Ok(Some(Tick(raw)))
        }
    }

    /// Store the latest WorldSnapshot produced during journal.commit().
    ///
    /// Called by the Journal after ingesting all table data. The stored snapshot
    /// can be retrieved by consumers (notably the bevy-bridge for entity
    /// materialization) via [`snapshot()`].
    pub fn store_world_snapshot(&self, snapshot: WorldSnapshot) -> QueryResult<()> {
        let mut guard = self
            .latest_snapshot
            .write()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        *guard = Some(Arc::new(snapshot));
        Ok(())
    }

    /// Obtain the latest WorldSnapshot reference.
    ///
    /// The snapshot is pushed into query-engine during journal.commit() via
    /// [`store_world_snapshot()`]. This is NOT a pull-based tick lookup — it
    /// returns whatever snapshot was most recently stored.
    ///
    /// The bevy-bridge uses this for entity materialization (it needs direct
    /// `snapshot.get_table()` access to iterate batches and spawn Bevy entities).
    /// All general-purpose data reads should use the typed read APIs
    /// ([`column_view`], [`lookup_row`], [`batch_reader`], [`read`]).
    pub fn snapshot(&self) -> QueryResult<Arc<WorldSnapshot>> {
        self.latest_snapshot
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?
            .clone()
            .ok_or_else(|| QueryError::InvalidQuery("no snapshot available".to_owned()))
    }

    // ------------------------------------------------------------------
    // Typed columnar access
    // ------------------------------------------------------------------

    pub fn column_view(&self, table_name: &str, column_name: &str) -> QueryResult<ColumnView> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let view = cache
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
        let cols = view.first_batch_columns()?;
        cols.into_iter()
            .find(|(name, _)| name == column_name)
            .map(|(_, v)| v)
            .ok_or_else(|| QueryError::ColumnNotFound {
                column: column_name.to_owned(),
                table: table_name.to_owned(),
            })
    }

    pub fn row_cursor(&self, table_name: &str) -> QueryResult<RowCursor> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let view = cache
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
        view.first_batch_cursor()
    }

    pub fn batch_reader(&self, table_name: &str) -> QueryResult<BatchColumnReader> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let view = cache
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
        view.first_batch_reader()
    }

    pub fn lookup_row(&self, table_name: &str, row_id: RowId) -> QueryResult<RowLookupView> {
        let tick = self
            .latest_tick()?
            .ok_or_else(|| QueryError::InvalidQuery("no tick available".to_owned()))?;
        let view = self.read_single_table(tick, table_name)?;
        let (batch_idx, offset) = {
            let pm = view.position_map();
            pm.position_of(row_id).ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "row {:?} not found in table '{}'",
                    row_id, table_name
                ))
            })?
        };
        let lookup = RowLookup::new(row_id, batch_idx, offset);
        Ok(RowLookupView::new(lookup, view))
    }

    // ------------------------------------------------------------------
    // SQL interface
    // ------------------------------------------------------------------

    pub fn sql_context(
        &self,
    ) -> QueryResult<impl std::ops::Deref<Target = SqlExecutionContext> + '_> {
        self.sql_context
            .read()
            .map_err(|_| QueryError::DataFusion("poisoned lock".to_owned()))
    }

    pub fn sql_context_mut(
        &self,
    ) -> QueryResult<impl std::ops::DerefMut<Target = SqlExecutionContext> + '_> {
        self.sql_context
            .write()
            .map_err(|_| QueryError::DataFusion("poisoned lock".to_owned()))
    }

    pub fn register_sql_table(&self, name: &str, batches: Vec<RecordBatch>) -> QueryResult<()> {
        self.sql_context_mut()?.register_table(name, batches)
    }

    pub async fn execute_sql(&self, sql: &str) -> QueryResult<Vec<RecordBatch>> {
        self.sql_context()?.execute_sql(sql).await
    }

    // ------------------------------------------------------------------
    // Debug write journal
    // ------------------------------------------------------------------

    pub fn debug_journal(&self) -> &DebugWriteJournal {
        &self.debug_journal
    }

    /// Execute a SQL write statement (UPDATE/INSERT/DELETE) through the debug write path.
    ///
    /// This is the compliant interception layer for debug builds:
    /// 1. Parse SQL into a [`scharnhorst_journal::sql_parser::SqlStatement`]
    /// 2. Convert to [`Diff`] using `SqlParser::statement_to_diff`
    /// 3. Record the operation in the [`DebugWriteJournal`] ring buffer
    /// 4. Submit the [`Diff`] to the journal-system for atomic commit
    ///
    /// Only available in debug builds (`#[cfg(debug_assertions)]`).
    /// In multiplayer sessions, the debug journal should be disabled via
    /// [`DebugWriteJournal::set_enabled(false)`].
    #[cfg(debug_assertions)]
    pub fn execute_sql_write(
        &self,
        sql: &str,
        journal: &mut scharnhorst_journal::Journal,
        tick: Tick,
    ) -> QueryResult<()> {
        use scharnhorst_journal::sql_parser::SqlParser;

        // Step 1: Parse SQL
        let stmt = SqlParser::parse(sql)
            .map_err(|e| QueryError::SqlWrite(format!("SQL parse error: {e}")))?;

        // Step 2: Convert to Diff
        let diff = SqlParser::statement_to_diff(stmt)
            .map_err(|e| QueryError::SqlWrite(format!("SQL to Diff error: {e}")))?;

        // Step 3: Record in debug journal ring buffer
        {
            let op = diff_to_debug_write_op(&diff, tick);
            self.debug_journal.record(op);
        }

        // Step 4: Submit to journal-system for atomic commit
        journal
            .submit_diff(diff, &JournalSubmitToken::new())
            .map_err(|e| QueryError::SqlWrite(format!("journal submit error: {e}")))?;

        Ok(())
    }

    // ------------------------------------------------------------------
    // Inspector / console (developer tooling)
    // ------------------------------------------------------------------

    pub fn inspector(&self) -> QueryResult<impl std::ops::Deref<Target = InspectorConsole> + '_> {
        self.inspector
            .read()
            .map_err(|_| QueryError::Inspector("poisoned lock".to_owned()))
    }

    pub fn inspector_mut(
        &self,
    ) -> QueryResult<impl std::ops::DerefMut<Target = InspectorConsole> + '_> {
        self.inspector
            .write()
            .map_err(|_| QueryError::Inspector("poisoned lock".to_owned()))
    }

    pub fn inspect_table_summary(&self, table_name: &str) -> QueryResult<TableSummary> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let view = cache
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
        let inspector = self.inspector()?;
        inspector.summarize_table(table_name, &view.batches().cloned().collect::<Vec<_>>())
    }

    pub fn inspect_table_page(
        &self,
        table_name: &str,
        page_index: usize,
        page_size: usize,
    ) -> QueryResult<InspectorPage> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let view = cache
            .get(table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
        let inspector = self.inspector()?;
        inspector.inspect_page(
            table_name,
            &view.batches().cloned().collect::<Vec<_>>(),
            page_index,
            page_size,
        )
    }

    // ------------------------------------------------------------------
    // Cache management
    // ------------------------------------------------------------------

    pub fn clear_cache(&self) -> QueryResult<()> {
        let mut cache = self
            .view_cache
            .write()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        cache.clear();
        Ok(())
    }

    pub fn cached_table_names(&self) -> QueryResult<Vec<String>> {
        let _tick = self.latest_tick()?;
        let cache = self
            .view_cache
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        Ok(cache.keys().cloned().collect())
    }

    // ------------------------------------------------------------------
    // Schema freeze check (for scheduler initialization guard)
    // ------------------------------------------------------------------

    /// Returns true if the underlying schema registry is frozen.
    ///
    /// The scheduler calls this during [`initialize`] to ensure the
    /// schema registry has been frozen before simulation starts.
    /// If the registry is not frozen, the scheduler returns an error.
    pub fn is_schema_frozen(&self) -> QueryResult<bool> {
        let reg = self.schema_registry()?;
        Ok(reg.is_frozen())
    }

    /// Freeze the underlying schema registry, preventing further table
    /// registrations. Returns an error if the registry lock is poisoned.
    pub fn freeze_schema_registry(&self) -> QueryResult<()> {
        let mut reg = self
            .schema_registry
            .write()
            .map_err(|_| QueryError::UnifiedRead("schema registry lock poisoned".to_owned()))?;
        reg.freeze();
        Ok(())
    }
}

impl UnifiedReadSource for QueryEngine {
    fn read(&self, request: ReadRequest) -> QueryResult<ReadResponse> {
        QueryEngine::read(self, request)
    }
}

impl SnapshotIngestor for QueryEngine {
    fn ingest_snapshot(
        &self,
        tick: Tick,
        table_name: &str,
        batches: Vec<RecordBatch>,
        position_map: RowPositionMap,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        QueryEngine::ingest_snapshot(self, tick, table_name, batches, position_map)?;
        Ok(())
    }

    fn store_snapshot(
        &self,
        snapshot: WorldSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        QueryEngine::store_world_snapshot(self, snapshot)?;
        Ok(())
    }
}

/// Map a [`Diff`] to a [`DebugWriteOp`] for recording in the debug journal
/// ring buffer.
///
/// Only available in debug builds.
#[cfg(debug_assertions)]
fn diff_to_debug_write_op(diff: &Diff, tick: Tick) -> DebugWriteOp {
    match diff {
        Diff::Update { table, .. } => DebugWriteOp::Patch {
            table: table.clone(),
            tick,
            row_indices: vec![],
        },
        Diff::Insert { table, .. } => DebugWriteOp::Append {
            table: table.clone(),
            tick,
            row_count: 1,
        },
        Diff::Delete { table, .. } => DebugWriteOp::Delete {
            table: table.clone(),
            tick,
            row_indices: vec![],
        },
        Diff::ReplaceTable { table, .. } => DebugWriteOp::Rebuild {
            table: table.clone(),
            tick,
            row_count: 0,
        },
    }
}
