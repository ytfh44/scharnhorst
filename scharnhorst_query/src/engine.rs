use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_schema;
use datafusion::prelude::SessionContext;
use scharnhorst_arrow_store::{SnapshotIngestRollback, SnapshotIngestor, WorldSnapshot, WorldView};
use scharnhorst_core::{Diff, JournalSubmitToken, RowId, RowLookup, RowPositionMap, Tick};
use scharnhorst_schema::{RelationEdge, SchemaRegistry, TableSpec};

use crate::debug_write::{DebugWriteJournal, DebugWriteOp};
use crate::error::{QueryError, QueryResult};
use crate::inspector::{InspectorConsole, InspectorPage, TableSummary};
use crate::sql_interface::SqlExecutionContext;
use crate::telemetry;
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
    /// Transactional staging area used by Journal snapshot publication.
    pending_ingest: Arc<Mutex<Option<PendingSnapshotIngest>>>,
    /// Access pattern tracking: table.column -> call count.
    /// Only compiled when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    access_counts: Arc<std::sync::Mutex<HashMap<String, u64>>>,
    /// Ring buffer of diff summaries for inspector tooling.
    #[cfg(debug_assertions)]
    diff_ring: Arc<Mutex<DiffSummaryRing>>,
}

/// A path to validate against the current schema.
/// Used by query_engine.validate() to check that all rule paths
/// reference valid tables, columns, and relations.
#[derive(Debug, Clone)]
pub struct ValidatablePath {
    /// Human-readable identifier for error reporting (e.g., "rule.tax.treasury")
    pub path: String,
    /// The table name referenced by this path
    pub table: String,
    /// Column names referenced by this path
    pub columns: Vec<String>,
    /// Relation keys in format "from_table -> to_table"
    pub relations: Vec<String>,
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
            pending_ingest: Arc::new(Mutex::new(None)),
            #[cfg(feature = "metrics")]
            access_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(debug_assertions)]
            diff_ring: Arc::new(Mutex::new(DiffSummaryRing::new(1024))),
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

    /// Validate all registered paths against the current schema.
    ///
    /// Checks:
    /// - Each table exists in SchemaRegistry
    /// - Each column exists on its table
    /// - Each relation exists in RelationGraph
    ///
    /// Returns Ok(empty_vec) if all paths are valid.
    /// Does NOT access view_cache, snapshots, or per-tick state.
    /// Idempotent — calling twice with same schema returns same result.
    pub fn validate(
        &self,
        paths: &[ValidatablePath],
    ) -> QueryResult<Vec<crate::error::ValidationError>> {
        use crate::error::{ValidationError, ValidationErrorKind};

        let reg = self.schema_registry()?;
        let mut errors = Vec::new();

        for p in paths {
            match reg.get(&p.table) {
                Ok(spec) => {
                    for col_name in &p.columns {
                        if spec.column_by_name(col_name).is_none() {
                            errors.push(ValidationError {
                                path: p.path.clone(),
                                kind: ValidationErrorKind::MissingColumn,
                                message: format!(
                                    "column '{}' not found in table '{}'",
                                    col_name, p.table
                                ),
                            });
                        }
                    }
                    let all_edges = reg.relation_graph().all_edges();
                    for rel_key in &p.relations {
                        let found = all_edges.iter().any(|e| {
                            let edge_key = format!("{} -> {}", e.from, e.to);
                            edge_key == *rel_key
                        });
                        if !found {
                            errors.push(ValidationError {
                                path: p.path.clone(),
                                kind: ValidationErrorKind::MissingRelation,
                                message: format!(
                                    "relation '{}' not found for path '{}'",
                                    rel_key, p.path
                                ),
                            });
                        }
                    }
                }
                Err(_) => {
                    errors.push(ValidationError {
                        path: p.path.clone(),
                        kind: ValidationErrorKind::MissingTable,
                        message: format!("table '{}' not found for path '{}'", p.table, p.path),
                    });
                }
            }
        }

        Ok(errors)
    }

    // ------------------------------------------------------------------
    // Unified read interface
    // ------------------------------------------------------------------

    pub fn read(&self, request: ReadRequest) -> QueryResult<ReadResponse> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();
        #[cfg(feature = "metrics")]
        let table_names_joined = request.table_names.join(",");

        let result = (|| -> QueryResult<ReadResponse> {
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
        })();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_api_call(
                "read",
                &table_names_joined,
                None,
                start.elapsed().as_micros() as u64,
            );
        }

        result
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

        {
            let mut pending = self
                .pending_ingest
                .lock()
                .map_err(|_| QueryError::UnifiedRead("pending ingest lock poisoned".to_owned()))?;
            if let Some(state) = pending.as_mut() {
                match state.tick {
                    Some(existing) if existing != tick => {
                        return Err(QueryError::TicksMismatch {
                            requested: tick,
                            actual: Some(existing),
                        });
                    }
                    None => state.tick = Some(tick),
                    _ => {}
                }
                state.views.insert(table_name.to_owned(), view);
                return Ok(());
            }
        }

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
        if snapshot.tick().as_u64() == u64::MAX {
            return Err(QueryError::UnsupportedOperation(
                "Tick sentinel collision: u64::MAX is reserved".to_owned(),
            ));
        }

        let staged_views = {
            let mut pending = self
                .pending_ingest
                .lock()
                .map_err(|_| QueryError::UnifiedRead("pending ingest lock poisoned".to_owned()))?;
            if let Some(state) = pending.as_ref() {
                if let Some(staged_tick) = state.tick {
                    if staged_tick != snapshot.tick() {
                        return Err(QueryError::TicksMismatch {
                            requested: snapshot.tick(),
                            actual: Some(staged_tick),
                        });
                    }
                }
            }
            pending.take().map(|state| state.views)
        };

        let publish_tick = staged_views.is_some();
        if let Some(views) = staged_views {
            let mut cache = self
                .view_cache
                .write()
                .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
            *cache = views;
        }

        let mut guard = self
            .latest_snapshot
            .write()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
        let tick = snapshot.tick();
        *guard = Some(Arc::new(snapshot));
        drop(guard);

        if publish_tick {
            self.latest_tick.store(tick.as_u64(), Ordering::Release);
        }
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
    pub fn snapshot(&self) -> QueryResult<WorldView> {
        let arc = self
            .latest_snapshot
            .read()
            .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?
            .clone()
            .ok_or_else(|| QueryError::InvalidQuery("no snapshot available".to_owned()))?;
        Ok(WorldView::new(arc))
    }

    // ------------------------------------------------------------------
    // Typed columnar access
    // ------------------------------------------------------------------

    pub fn column_view(&self, table_name: &str, column_name: &str) -> QueryResult<ColumnView> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = (|| -> QueryResult<ColumnView> {
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
        })();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_api_call(
                "column_view",
                table_name,
                Some(column_name),
                start.elapsed().as_micros() as u64,
            );
            if result.is_ok() {
                telemetry::record_cache_hit();
            } else {
                telemetry::record_cache_miss();
            }
            if let Ok(mut map) = self.access_counts.lock() {
                let key = format!("{}.{}", table_name, column_name);
                *map.entry(key).or_insert(0) += 1;
            }
        }

        result
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
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = (|| -> QueryResult<BatchColumnReader> {
            let _tick = self.latest_tick()?;
            let cache = self
                .view_cache
                .read()
                .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
            let view = cache
                .get(table_name)
                .ok_or_else(|| QueryError::TableNotFound(table_name.to_owned()))?;
            view.first_batch_reader()
        })();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_api_call(
                "batch_reader",
                table_name,
                None,
                start.elapsed().as_micros() as u64,
            );
            if let Ok(mut map) = self.access_counts.lock() {
                *map.entry(table_name.to_owned()).or_insert(0) += 1;
            }
        }

        result
    }

    pub fn lookup_row(&self, table_name: &str, row_id: RowId) -> QueryResult<RowLookupView> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let result = (|| -> QueryResult<RowLookupView> {
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
        })();

        #[cfg(feature = "metrics")]
        {
            telemetry::record_api_call(
                "lookup_row",
                table_name,
                None,
                start.elapsed().as_micros() as u64,
            );
            if let Ok(mut map) = self.access_counts.lock() {
                *map.entry(table_name.to_owned()).or_insert(0) += 1;
            }
        }

        result
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

    /// Execute a SELECT-only SQL query against the current snapshot.
    ///
    /// Creates a fresh DataFusion SessionContext per invocation (I-SQL-FRESH-SESSION).
    /// Rejects non-SELECT statements before DataFusion execution.
    /// Rejects multi-statement SQL (semicolons).
    ///
    /// Returns a single RecordBatch with the query result.
    pub fn sql(&self, sql_str: &str) -> QueryResult<RecordBatch> {
        let sql_trimmed = sql_str.trim();

        if sql_trimmed.is_empty() {
            return Err(QueryError::SqlNotAllowed("empty SQL statement".to_owned()));
        }

        let cleaned = strip_single_trailing_statement_semicolon(sql_trimmed)?;

        // Non-SELECT check: get first word (uppercase)
        let first_word = cleaned
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_uppercase();

        if first_word != "SELECT" && first_word != "WITH" {
            return Err(QueryError::SqlNotAllowed(format!(
                "only SELECT/WITH queries are allowed, got: {}",
                first_word
            )));
        }

        // Fresh SessionContext
        let ctx = SessionContext::new();

        // Register tables from view_cache
        {
            let cache = self
                .view_cache
                .read()
                .map_err(|_| QueryError::UnifiedRead("poisoned lock".to_owned()))?;
            for (name, view) in cache.iter() {
                let batches: Vec<RecordBatch> = view.batches().cloned().collect();
                if batches.is_empty() {
                    ctx.register_batch(
                        name,
                        RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
                    )
                    .map_err(|e| QueryError::DataFusion(e.to_string()))?;
                } else {
                    let schema = batches[0].schema();
                    let merged = concat_batches(&schema, &batches)
                        .map_err(|e| QueryError::DataFusion(e.to_string()))?;
                    ctx.register_batch(name, merged)
                        .map_err(|e| QueryError::DataFusion(e.to_string()))?;
                }
            }
        }

        // Execute
        let df = pollster::block_on(ctx.sql(cleaned))
            .map_err(|e| QueryError::Sql(format!("DataFusion error: {e}")))?;
        let batches = pollster::block_on(df.collect())
            .map_err(|e| QueryError::Sql(format!("DataFusion error: {e}")))?;

        if batches.is_empty() {
            Ok(RecordBatch::new_empty(Arc::new(
                arrow_schema::Schema::empty(),
            )))
        } else {
            let schema = batches[0].schema();
            let merged = concat_batches(&schema, &batches)
                .map_err(|e| QueryError::DataFusion(e.to_string()))?;
            Ok(merged)
        }
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
    // Telemetry summary (called by scheduler at tick boundaries)
    // ------------------------------------------------------------------

    /// Emit telemetry summary at tick end (no-op when metrics disabled).
    pub fn emit_telemetry_summary(&self, tick: u64) {
        telemetry::emit_cache_summary(tick);
        #[cfg(feature = "metrics")]
        {
            if let Ok(map) = self.access_counts.lock() {
                if !map.is_empty() {
                    tracing::info!(
                        tick = tick,
                        access_pattern = ?*map,
                        "access pattern summary"
                    );
                }
            }
        }
    }

    /// Reset telemetry counters at tick start (no-op when metrics disabled).
    pub fn reset_telemetry_counters(&self) {
        telemetry::reset_cache_counters();
        #[cfg(feature = "metrics")]
        {
            if let Ok(mut map) = self.access_counts.lock() {
                map.clear();
            }
        }
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

fn strip_single_trailing_statement_semicolon(sql: &str) -> QueryResult<&str> {
    let mut quote: Option<char> = None;
    let mut semicolon_index: Option<usize> = None;
    let mut semicolon_count = 0usize;
    let mut chars = sql.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                if active_quote == '\''
                    && chars
                        .peek()
                        .map(|(_, next)| *next == active_quote)
                        .unwrap_or(false)
                {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            ';' => {
                semicolon_count += 1;
                semicolon_index = Some(idx);
            }
            _ => {}
        }
    }

    match (semicolon_count, semicolon_index) {
        (0, _) => Ok(sql),
        (1, Some(idx)) if sql[idx + 1..].trim().is_empty() => Ok(sql[..idx].trim_end()),
        _ => Err(QueryError::SqlNotAllowed(
            "multi-statement SQL not allowed".to_owned(),
        )),
    }
}

#[derive(Debug)]
struct PendingSnapshotIngest {
    tick: Option<Tick>,
    views: HashMap<String, TableReadView>,
}

impl PendingSnapshotIngest {
    fn new() -> Self {
        Self {
            tick: None,
            views: HashMap::new(),
        }
    }
}

struct QueryEngineIngestRollback {
    view_cache: Arc<RwLock<HashMap<String, TableReadView>>>,
    latest_tick: Arc<AtomicU64>,
    latest_snapshot: Arc<RwLock<Option<Arc<WorldSnapshot>>>>,
    pending_ingest: Arc<Mutex<Option<PendingSnapshotIngest>>>,
    saved_cache: HashMap<String, TableReadView>,
    saved_tick: u64,
    saved_snapshot: Option<Arc<WorldSnapshot>>,
}

impl SnapshotIngestRollback for QueryEngineIngestRollback {
    fn rollback(self: Box<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        {
            let mut cache = self.view_cache.write().map_err(|_| {
                Box::new(std::io::Error::other(
                    "query view cache lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            *cache = self.saved_cache;
        }

        {
            let mut snapshot = self.latest_snapshot.write().map_err(|_| {
                Box::new(std::io::Error::other(
                    "query snapshot lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            *snapshot = self.saved_snapshot;
        }

        {
            let mut pending = self.pending_ingest.lock().map_err(|_| {
                Box::new(std::io::Error::other(
                    "query pending ingest lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            *pending = None;
        }

        self.latest_tick.store(self.saved_tick, Ordering::Release);
        Ok(())
    }
}

impl SnapshotIngestor for QueryEngine {
    fn begin_ingest(
        &self,
    ) -> Result<Box<dyn SnapshotIngestRollback>, Box<dyn std::error::Error + Send + Sync>> {
        let saved_cache = self
            .view_cache
            .read()
            .map_err(|_| {
                Box::new(std::io::Error::other(
                    "query view cache lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?
            .clone();

        let saved_snapshot = self
            .latest_snapshot
            .read()
            .map_err(|_| {
                Box::new(std::io::Error::other(
                    "query snapshot lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?
            .clone();

        {
            let mut pending = self.pending_ingest.lock().map_err(|_| {
                Box::new(std::io::Error::other(
                    "query pending ingest lock poisoned",
                )) as Box<dyn std::error::Error + Send + Sync>
            })?;
            if pending.is_some() {
                return Err(Box::new(std::io::Error::other(
                    "query ingest already active",
                )));
            }
            *pending = Some(PendingSnapshotIngest::new());
        }

        Ok(Box::new(QueryEngineIngestRollback {
            view_cache: Arc::clone(&self.view_cache),
            latest_tick: Arc::clone(&self.latest_tick),
            latest_snapshot: Arc::clone(&self.latest_snapshot),
            pending_ingest: Arc::clone(&self.pending_ingest),
            saved_cache,
            saved_tick: self.latest_tick.load(Ordering::Acquire),
            saved_snapshot,
        }))
    }

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

// ------------------------------------------------------------------
// Diff summary ring buffer (inspector tooling, debug builds only)
// ------------------------------------------------------------------

/// A single diff summary entry for the inspector diff stream panel.
#[cfg(debug_assertions)]
#[derive(Debug, Clone)]
pub struct DiffSummary {
    pub tick: u64,
    pub table_name: String,
    pub diff_kind: String,
    pub summary: String,
}

/// A fixed-size ring buffer of `DiffSummary` entries.
#[cfg(debug_assertions)]
#[derive(Debug, Clone)]
pub struct DiffSummaryRing {
    capacity: usize,
    entries: std::collections::VecDeque<DiffSummary>,
}

#[cfg(debug_assertions)]
impl DiffSummaryRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: std::collections::VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, summaries: impl IntoIterator<Item = DiffSummary>) {
        for entry in summaries {
            while self.entries.len() >= self.capacity {
                self.entries.pop_front();
            }
            self.entries.push_back(entry);
        }
    }

    pub fn snapshot(&self) -> Vec<DiffSummary> {
        self.entries.iter().cloned().collect()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(debug_assertions)]
impl QueryEngine {
    /// Push diff summaries into the ring buffer.
    ///
    /// Called by the scheduler after each successful atomic commit.
    pub fn push_diff_summaries(&self, diffs: &[Diff], tick: Tick) {
        let summaries: Vec<DiffSummary> = diffs
            .iter()
            .map(|d| {
                let table_name = d.table().to_owned();
                let (kind, summary) = match d {
                    Diff::Update {
                        table: _,
                        row,
                        column,
                        value,
                    } => {
                        let s = format!("{}.{}@{} = {}", table_name, column, row.as_u64(), value);
                        ("Update".to_owned(), s)
                    }
                    Diff::Insert {
                        table: _,
                        row,
                        values,
                    } => {
                        let s = format!(
                            "insert into {} ({} cols) @{}",
                            table_name,
                            values.len(),
                            row.as_u64()
                        );
                        ("Insert".to_owned(), s)
                    }
                    Diff::Delete { table: _, row } => {
                        let s = format!("delete from {} @{}", table_name, row.as_u64());
                        ("Delete".to_owned(), s)
                    }
                    Diff::ReplaceTable { table: _, rows } => {
                        let s = format!("replace {} ({} rows)", table_name, rows.len());
                        ("ReplaceTable".to_owned(), s)
                    }
                };
                DiffSummary {
                    tick: tick.as_u64(),
                    table_name,
                    diff_kind: kind,
                    summary,
                }
            })
            .collect();

        if let Ok(mut ring) = self.diff_ring.lock() {
            ring.push(summaries);
        }
    }

    /// Return a snapshot of all diff summaries in the ring buffer.
    pub fn diff_summaries(&self) -> Vec<DiffSummary> {
        self.diff_ring
            .lock()
            .map(|ring| ring.snapshot())
            .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use scharnhorst_arrow_store::{MutationMode, VersionedTable, WorldSnapshot, WorldView};

    fn make_snapshot_with_heroes(tick_val: u64) -> WorldSnapshot {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20]));
        let name_arr: arrow_array::ArrayRef =
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));

        let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();

        let mut table = VersionedTable::new("heroes", MutationMode::AppendOnly);
        table.insert_version(Tick(tick_val), vec![batch]).unwrap();
        WorldSnapshot::with_table(Tick(tick_val), "heroes", Arc::new(table))
    }

    #[test]
    fn snapshot_returns_world_view_not_arc_world_snapshot() {
        let engine = QueryEngine::new(SchemaRegistry::new());
        let snap = make_snapshot_with_heroes(5);
        engine.store_world_snapshot(snap).unwrap();

        let view: WorldView = engine.snapshot().unwrap();
        // If this compiles, the return type is WorldView, not Arc<WorldSnapshot>.
        assert_eq!(view.tick(), Tick(5));
        assert!(view.table_names().contains(&"heroes".to_owned()));
    }

    #[test]
    fn snapshot_returns_error_when_no_snapshot_stored() {
        let engine = QueryEngine::new(SchemaRegistry::new());
        let result = engine.snapshot();
        assert!(result.is_err());
    }

    /// WorldView must NOT expose RecordBatch or raw Arrow types.
    /// This test exercises every public method and verifies all return
    /// types are plain Rust types, not Arrow internals.
    #[test]
    fn world_view_encapsulation_no_recordbatch_leak() {
        let engine = QueryEngine::new(SchemaRegistry::new());
        let snap = make_snapshot_with_heroes(1);
        engine.store_world_snapshot(snap).unwrap();
        let view = engine.snapshot().unwrap();

        // Every method returns non-Arrow types.
        let _: scharnhorst_core::Tick = view.tick();
        let _: Vec<String> = view.table_names();
        let _: usize = view.row_count("heroes").unwrap();
        let _: Vec<String> = view.column_names("heroes").unwrap();
        let _: String = view.column_type("heroes", "id").unwrap();

        let iter = view.iter_rows("heroes").unwrap();
        for row_result in iter {
            let row = row_result.unwrap();
            let _: Option<i64> = row.get_i64(0).unwrap();
            let _: Option<String> = row.get_string(1).unwrap();
        }
    }

    /// I-WV-IMMUTABLE: A WorldView obtained at tick N remains stale
    /// after store_world_snapshot pushes newer data.
    #[test]
    fn world_view_held_across_tick_boundary_returns_stale_data() {
        let engine = QueryEngine::new(SchemaRegistry::new());

        // Store tick-5 snapshot with table "old_data"
        let snap5 = WorldSnapshot::with_table(
            Tick(5),
            "old_data",
            Arc::new(VersionedTable::new("old_data", MutationMode::AppendOnly)),
        );
        engine.store_world_snapshot(snap5).unwrap();

        let view = engine.snapshot().unwrap();
        assert_eq!(view.tick(), Tick(5));
        assert!(view.table_names().contains(&"old_data".to_owned()));

        // Store tick-10 snapshot with table "new_data"
        let snap10 = WorldSnapshot::with_table(
            Tick(10),
            "new_data",
            Arc::new(VersionedTable::new("new_data", MutationMode::AppendOnly)),
        );
        engine.store_world_snapshot(snap10).unwrap();

        // view still sees tick-5 data
        assert_eq!(view.tick(), Tick(5));
        assert!(view.table_names().contains(&"old_data".to_owned()));
        assert!(!view.table_names().contains(&"new_data".to_owned()));

        // Fresh snapshot sees tick-10 data
        let fresh = engine.snapshot().unwrap();
        assert_eq!(fresh.tick(), Tick(10));
        assert!(fresh.table_names().contains(&"new_data".to_owned()));
        assert!(!fresh.table_names().contains(&"old_data".to_owned()));
    }

    #[test]
    fn snapshot_via_store_world_snapshot_propagates_table_data() {
        let engine = QueryEngine::new(SchemaRegistry::new());
        let snap = make_snapshot_with_heroes(3);
        engine.store_world_snapshot(snap).unwrap();

        let view = engine.snapshot().unwrap();
        assert_eq!(view.row_count("heroes").unwrap(), 2);

        let iter = view.iter_rows("heroes").unwrap();
        let rows: Vec<_> = iter.map(|r| r.unwrap()).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_i64(0).unwrap(), Some(10));
        assert_eq!(rows[0].get_string(1).unwrap(), Some("alpha".to_owned()));
        assert_eq!(rows[1].get_i64(0).unwrap(), Some(20));
        assert_eq!(rows[1].get_string(1).unwrap(), Some("beta".to_owned()));
    }

    // ------------------------------------------------------------------
    // validate() tests
    // ------------------------------------------------------------------

    use crate::error::ValidationErrorKind;

    fn make_spec_with_columns(name: &str, col_names: &[&str]) -> TableSpec {
        let mut spec = TableSpec::new(name);
        for col_name in col_names {
            spec = spec
                .with_column(scharnhorst_schema::ColumnSpec::new(
                    *col_name,
                    scharnhorst_schema::FieldSemantic::Quantity,
                    "i64",
                ))
                .unwrap();
        }
        spec
    }

    #[test]
    fn validate_passes_on_valid_schema() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("heroes", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);
        let paths = vec![ValidatablePath {
            path: "test.heroes".into(),
            table: "heroes".into(),
            columns: vec!["id".into(), "name".into()],
            relations: vec![],
        }];
        let errors = qe.validate(&paths).unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn validate_catches_missing_table() {
        let reg = SchemaRegistry::new();
        let qe = QueryEngine::new(reg);
        let paths = vec![ValidatablePath {
            path: "test.missing".into(),
            table: "nonexistent".into(),
            columns: vec![],
            relations: vec![],
        }];
        let errors = qe.validate(&paths).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, ValidationErrorKind::MissingTable);
    }

    #[test]
    fn validate_catches_missing_column() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("heroes", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);
        let paths = vec![ValidatablePath {
            path: "test.heroes".into(),
            table: "heroes".into(),
            columns: vec!["nonexistent_col".into()],
            relations: vec![],
        }];
        let errors = qe.validate(&paths).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, ValidationErrorKind::MissingColumn);
    }

    #[test]
    fn validate_catches_missing_relation() {
        let mut reg = SchemaRegistry::new();
        let spec_a = make_spec_with_columns("A", &["id"]);
        let spec_b = make_spec_with_columns("B", &["id"]);
        reg.register(spec_a).unwrap();
        reg.register(spec_b).unwrap();
        reg.add_relation(scharnhorst_schema::RelationEdge {
            from: "A".into(),
            to: "B".into(),
            kind: scharnhorst_schema::RelationKind::OneToMany,
            from_column: "id".into(),
            to_column: None,
        })
        .unwrap();
        let qe = QueryEngine::new(reg);
        let paths = vec![ValidatablePath {
            path: "test.missing_rel".into(),
            table: "A".into(),
            columns: vec![],
            relations: vec!["C -> D".into()],
        }];
        let errors = qe.validate(&paths).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].kind, ValidationErrorKind::MissingRelation);
    }

    #[test]
    fn validate_empty_paths_returns_empty() {
        let reg = SchemaRegistry::new();
        let qe = QueryEngine::new(reg);
        let errors = qe.validate(&[]).unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn validate_is_idempotent() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("heroes", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);
        let paths = vec![ValidatablePath {
            path: "test.heroes".into(),
            table: "heroes".into(),
            columns: vec!["id".into(), "name".into()],
            relations: vec![],
        }];
        let errors1 = qe.validate(&paths).unwrap();
        let errors2 = qe.validate(&paths).unwrap();
        assert_eq!(errors1, errors2);
    }

    // ------------------------------------------------------------------
    // sql() tests
    // ------------------------------------------------------------------

    #[test]
    fn sql_rejects_insert() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql("INSERT INTO test VALUES (1)").unwrap_err();
        assert!(matches!(err, QueryError::SqlNotAllowed(_)));
    }

    #[test]
    fn sql_rejects_update_delete() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err_update = qe.sql("UPDATE test SET x = 1").unwrap_err();
        assert!(matches!(err_update, QueryError::SqlNotAllowed(_)));
        let err_delete = qe.sql("DELETE FROM test").unwrap_err();
        assert!(matches!(err_delete, QueryError::SqlNotAllowed(_)));
    }

    #[test]
    fn sql_rejects_multi_statement() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql("SELECT 1; SELECT 2").unwrap_err();
        assert!(matches!(err, QueryError::SqlNotAllowed(_)));
    }

    #[test]
    fn sql_rejects_empty() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql("").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn sql_allows_trailing_semicolon() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        // "SELECT 1;" with trailing semicolon should succeed
        // No tables registered — DataFusion should still be able to
        // evaluate "SELECT 1"
        let result = qe.sql("SELECT 1;");
        // This should not return SqlNotAllowed
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    /// May-fail: semicolons inside string literals are data, not statements.
    #[test]
    fn sql_allows_semicolon_inside_string_literal() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let result = qe.sql("SELECT ';' AS semi");
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    #[test]
    fn sql_select_returns_data() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("heroes", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);

        // Ingest data into view_cache
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        let name_arr: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![
            Some("alpha"),
            Some("beta"),
            Some("gamma"),
        ]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();
        let pm = scharnhorst_core::RowPositionMap::default();
        qe.ingest_snapshot(Tick(1), "heroes", vec![batch], pm)
            .unwrap();

        let result = qe.sql("SELECT * FROM heroes");
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    /// sql(";") — semicolon-only input reaches the first-word check
    /// as empty string and MUST be rejected.
    #[test]
    fn sql_rejects_semicolon_only() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql(";").unwrap_err();
        assert!(
            matches!(err, QueryError::SqlNotAllowed(_)),
            "expected SqlNotAllowed, got {:?}",
            err
        );
    }

    /// sql(";;") — double semicolon triggers multi-statement
    /// detection (semicolon_count > 1).
    #[test]
    fn sql_rejects_double_semicolon() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql(";;").unwrap_err();
        assert!(
            matches!(err, QueryError::SqlNotAllowed(_)),
            "expected SqlNotAllowed, got {:?}",
            err
        );
    }

    /// sql(" ; ") — whitespace with semicolon should also be rejected
    /// because stripping semicolons leaves only whitespace, which
    /// gives an empty first word.
    #[test]
    fn sql_rejects_whitespace_with_semicolon() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql(" ; ").unwrap_err();
        assert!(
            matches!(err, QueryError::SqlNotAllowed(_)),
            "expected SqlNotAllowed, got {:?}",
            err
        );
    }

    /// sql("WITH cte AS (SELECT 1) SELECT * FROM cte") — CTE queries
    /// starting with WITH must be accepted (not SqlNotAllowed).
    #[test]
    fn sql_allows_cte_with() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let result = qe.sql("WITH cte AS (SELECT 1 AS val) SELECT * FROM cte");
        assert!(
            result.is_ok(),
            "CTE WITH queries should be accepted, got {:?}",
            result.err()
        );
    }

    /// sql("SELECT 1; SELECT 2;") — multiple statements with trailing
    /// semicolon: strip_suffix removes one, but semicolon_count still
    /// shows more than 1.
    #[test]
    fn sql_rejects_multi_statement_with_trailing() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let err = qe.sql("SELECT 1; SELECT 2;").unwrap_err();
        assert!(
            matches!(err, QueryError::SqlNotAllowed(_)),
            "expected SqlNotAllowed, got {:?}",
            err
        );
    }

    // ------------------------------------------------------------------
    // metrics tests
    // ------------------------------------------------------------------

    #[cfg(feature = "metrics")]
    mod metrics_tests {
        use super::*;
        use std::sync::atomic::Ordering;

        #[test]
        fn telemetry_record_api_call_no_panic() {
            telemetry::record_api_call("test", "test_table", None, 100);
        }

        #[test]
        fn cache_counters_increment() {
            telemetry::reset_cache_counters();
            assert_eq!(telemetry::CACHE_HITS.load(Ordering::Relaxed), 0);
            assert_eq!(telemetry::CACHE_MISSES.load(Ordering::Relaxed), 0);
            telemetry::record_cache_hit();
            telemetry::record_cache_hit();
            telemetry::record_cache_miss();
            assert_eq!(telemetry::CACHE_HITS.load(Ordering::Relaxed), 2);
            assert_eq!(telemetry::CACHE_MISSES.load(Ordering::Relaxed), 1);
            telemetry::emit_cache_summary(1);
        }

        #[test]
        fn access_counts_increment_on_engine() {
            let qe = QueryEngine::new(SchemaRegistry::new());
            // Access counts start empty
            {
                let map = qe.access_counts.lock().unwrap();
                assert!(map.is_empty());
            }
            // Record some accesses
            {
                let mut map = qe.access_counts.lock().unwrap();
                *map.entry("heroes.name".to_owned()).or_insert(0) += 1;
                *map.entry("heroes.name".to_owned()).or_insert(0) += 1;
                *map.entry("heroes.level".to_owned()).or_insert(0) += 1;
            }
            // Verify counts
            {
                let map = qe.access_counts.lock().unwrap();
                assert_eq!(map.get("heroes.name"), Some(&2));
                assert_eq!(map.get("heroes.level"), Some(&1));
            }
        }

        #[test]
        fn emit_telemetry_summary_no_panic() {
            let qe = QueryEngine::new(SchemaRegistry::new());
            qe.emit_telemetry_summary(1);
            qe.reset_telemetry_counters();
        }

        /// emit_cache_summary with total==0 hits/cache_misses must use
        /// hit_ratio=1.0 (the else branch of total > 0 check).
        /// NOTE: does not assert exact counter values because CACHE_HITS
        /// and CACHE_MISSES are shared global statics across all tests.
        #[test]
        fn emit_cache_summary_with_total_zero() {
            telemetry::reset_cache_counters();
            // Verify emit_cache_summary does not panic when total==0.
            // The else branch sets hit_ratio=1.0 when hits+misses==0.
            telemetry::emit_cache_summary(42);
        }
    }

    // ------------------------------------------------------------------
    // May-fail: uncovered branch edge cases
    // ------------------------------------------------------------------

    /// May-fail: store_world_snapshot must reject Tick::MAX (u64::MAX)
    /// to prevent sentinel collision with latest_tick's None marker.
    ///
    /// Worst case: if Tick::MAX is accepted, latest_tick() returns None
    /// even though a snapshot exists, breaking all typed read paths.
    #[test]
    fn store_world_snapshot_rejects_max_tick() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let snap = make_snapshot_with_heroes(u64::MAX);
        let result = qe.store_world_snapshot(snap);
        assert!(
            result.is_err(),
            "store_world_snapshot must reject Tick::MAX (sentinel collision)"
        );
    }

    /// May-fail: ingest_snapshot without begin_ingest must still accept data.
    ///
    /// Worst case: if ingest_snapshot fails when pending_ingest is None
    /// (direct ingestion path), data cannot be ingested at all.
    #[test]
    fn ingest_snapshot_direct_path_accepts_data() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("direct", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        let name_arr: arrow_array::ArrayRef =
            Arc::new(StringArray::from(vec![Some("direct")]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, name_arr]).unwrap();
        let pm = scharnhorst_core::RowPositionMap::default();

        let result = qe.ingest_snapshot(Tick(42), "direct", vec![batch], pm);
        assert!(
            result.is_ok(),
            "direct ingest_snapshot (no begin_ingest) must succeed"
        );

        assert_eq!(qe.latest_tick().unwrap(), Some(Tick(42)));
        assert!(qe.cached_table_names().unwrap().contains(&"direct".to_owned()));
    }

    /// May-fail: ingest_snapshot rejects Tick::MAX in direct path.
    ///
    /// Worst case: u64::MAX tick overwrites the sentinel, making latest_tick()
    /// return None, breaking all consumers that depend on tick awareness.
    #[test]
    fn ingest_snapshot_rejects_max_tick() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("sentinel", &["id"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let id_arr: arrow_array::ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
        let batch = RecordBatch::try_new(schema, vec![id_arr]).unwrap();
        let pm = scharnhorst_core::RowPositionMap::default();

        let result = qe.ingest_snapshot(Tick(u64::MAX), "sentinel", vec![batch], pm);
        assert!(
            result.is_err(),
            "ingest_snapshot must reject Tick::MAX (sentinel collision)"
        );
    }

    /// May-fail: validate with mixed valid/invalid paths accumulates all errors.
    ///
    /// Worst case: if validate short-circuits on first error, users see only
    /// one problem at a time instead of full diagnostics.
    #[test]
    fn validate_accumulates_multiple_error_kinds() {
        let mut reg = SchemaRegistry::new();
        let spec = make_spec_with_columns("heroes", &["id", "name"]);
        reg.register(spec).unwrap();
        let qe = QueryEngine::new(reg);

        let paths = vec![
            // Valid path
            ValidatablePath {
                path: "ok.heroes".into(),
                table: "heroes".into(),
                columns: vec!["id".into()],
                relations: vec![],
            },
            // Missing table
            ValidatablePath {
                path: "bad.missing_table".into(),
                table: "nonexistent".into(),
                columns: vec![],
                relations: vec![],
            },
            // Missing column on existing table
            ValidatablePath {
                path: "bad.missing_col".into(),
                table: "heroes".into(),
                columns: vec!["nope".into()],
                relations: vec![],
            },
            // Missing relation
            ValidatablePath {
                path: "bad.missing_rel".into(),
                table: "heroes".into(),
                columns: vec![],
                relations: vec!["heroes -> monsters".into()],
            },
        ];

        let errors = qe.validate(&paths).unwrap();
        assert_eq!(
            errors.len(),
            3,
            "validate must accumulate all 3 errors, not just the first"
        );

        let kinds: Vec<&ValidationErrorKind> = errors.iter().map(|e| &e.kind).collect();
        assert!(kinds.iter().any(|k| matches!(k, ValidationErrorKind::MissingTable)));
        assert!(kinds.iter().any(|k| matches!(k, ValidationErrorKind::MissingColumn)));
        assert!(kinds.iter().any(|k| matches!(k, ValidationErrorKind::MissingRelation)));
    }

    /// May-fail: sql() with semicolon inside a string literal must not
    /// treat it as a statement terminator, avoiding spurious
    /// "multi-statement SQL" rejections.
    #[test]
    fn sql_allows_semicolon_in_string_literal() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let result = qe.sql("SELECT ';' AS col");
        assert!(
            result.is_ok(),
            "semicolon inside single-quoted string literal must be treated as data, got {:?}",
            result.err()
        );
    }
}

// ------------------------------------------------------------------
// DiffSummary ring buffer tests (debug_assertions only)
// ------------------------------------------------------------------

#[cfg(all(test, debug_assertions))]
mod diff_summary_tests {
    use super::*;
    use scharnhorst_core::RowId;

    #[test]
    fn push_and_read_diff_summaries() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let diffs = vec![
            Diff::Insert {
                table: "heroes".into(),
                row: RowId::new(1),
                values: {
                    let mut m = serde_json::Map::new();
                    m.insert("name".into(), serde_json::json!("alpha"));
                    m
                },
            },
            Diff::Update {
                table: "heroes".into(),
                row: RowId::new(2),
                column: "hp".into(),
                value: serde_json::json!(100),
            },
            Diff::Delete {
                table: "heroes".into(),
                row: RowId::new(3),
            },
            Diff::ReplaceTable {
                table: "items".into(),
                rows: vec![],
            },
        ];

        qe.push_diff_summaries(&diffs, Tick(5));
        let summaries = qe.diff_summaries();
        assert_eq!(summaries.len(), 4);
        assert_eq!(summaries[0].tick, 5);
        assert_eq!(summaries[0].table_name, "heroes");
        assert_eq!(summaries[0].diff_kind, "Insert");
        assert_eq!(summaries[1].diff_kind, "Update");
        assert_eq!(summaries[2].diff_kind, "Delete");
        assert_eq!(summaries[3].diff_kind, "ReplaceTable");
        assert_eq!(summaries[3].table_name, "items");
    }

    #[test]
    fn empty_diffs_produces_no_summaries() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        qe.push_diff_summaries(&[], Tick(0));
        let summaries = qe.diff_summaries();
        assert!(summaries.is_empty());
    }

    #[test]
    fn diff_summaries_starts_empty() {
        let qe = QueryEngine::new(SchemaRegistry::new());
        let summaries = qe.diff_summaries();
        assert!(summaries.is_empty());
    }
}
