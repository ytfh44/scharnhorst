use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::error::{QueryError, QueryResult};

/// A SQL execution context backed by DataFusion.
///
/// Registers simulation tables as DataFusion `TableProvider` instances and
/// exposes SQL-based analysis.
#[derive(Clone)]
pub struct SqlExecutionContext {
    ctx: SessionContext,
    tables: Arc<std::sync::Mutex<HashSet<String>>>,
}

impl SqlExecutionContext {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
            tables: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

 /// Register a named table so that it can be referenced in SQL queries.
    pub fn register_table(&self, name: &str, batches: Vec<RecordBatch>) -> QueryResult<()> {
        if name.is_empty() {
            return Err(QueryError::InvalidQuery(
                "cannot register table with empty name".to_owned(),
            ));
        }
        if batches.is_empty() {
            self.ctx
                .register_batch(name, RecordBatch::new_empty(Arc::new(
                    arrow_schema::Schema::empty(),
                )))
                .map_err(|e| QueryError::DataFusion(e.to_string()))?;
        } else {
            let schema = batches[0].schema();
            let merged =
                concat_batches(&schema, &batches)
                    .map_err(|e| QueryError::DataFusion(e.to_string()))?;
            self.ctx
                .register_batch(name, merged)
                .map_err(|e| QueryError::DataFusion(e.to_string()))?;
        }

        let mut guard = self
            .tables
            .lock()
            .map_err(|_| QueryError::DataFusion("poisoned lock".to_owned()))?;
        guard.insert(name.to_owned());
        Ok(())
    }

 /// Unregister a previously registered table.
    pub fn unregister_table(&self, name: &str) -> QueryResult<()> {
        self.ctx
            .deregister_table(name)
            .map_err(|e| QueryError::DataFusion(e.to_string()))?;

        let mut guard = self
            .tables
            .lock()
            .map_err(|_| QueryError::DataFusion("poisoned lock".to_owned()))?;
        guard.remove(name);
        Ok(())
    }

 /// Execute a SQL query and return the resulting record batches.
    pub async fn execute_sql(&self, sql: &str) -> QueryResult<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| QueryError::DataFusion(e.to_string()))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| QueryError::DataFusion(e.to_string()))?;

        Ok(batches)
    }

 /// Execute a SQL query and return at most `limit` rows.
    pub async fn execute_sql_limited(
        &self,
        sql: &str,
        limit: usize,
    ) -> QueryResult<Vec<RecordBatch>> {
        let limited_sql = format!("SELECT * FROM ({}) LIMIT {}", sql, limit);
        let batches = self.execute_sql(&limited_sql).await?;
        Ok(batches)
    }

 /// Return the list of tables currently registered in this context.
    pub fn registered_tables(&self) -> QueryResult<Vec<String>> {
        let guard = self
            .tables
            .lock()
            .map_err(|_| QueryError::DataFusion("poisoned lock".to_owned()))?;
        let mut names: Vec<String> = guard.iter().cloned().collect();
        names.sort();
        Ok(names)
    }
}

impl std::fmt::Debug for SqlExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlExecutionContext")
            .field("tables", &self.tables)
            .finish()
    }
}

impl Default for SqlExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

/// A pre-prepared SQL statement handle.
#[derive(Debug, Clone)]
pub struct PreparedSql {
    pub sql: String,
}

impl PreparedSql {
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }

 /// Execute the prepared statement against the given context.
    pub async fn execute(&self, ctx: &SqlExecutionContext) -> QueryResult<Vec<RecordBatch>> {
        ctx.execute_sql(&self.sql).await
    }
}
