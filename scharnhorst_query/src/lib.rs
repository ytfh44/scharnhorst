//! scharnhorst_query: read-only query engine, typed columnar access, SQL interface,
//! debug write journal, and inspector console.

pub mod debug_write;
pub mod engine;
pub mod error;
pub mod inspector;
pub mod sql_interface;
pub mod typed_access;
pub mod unified_read;

pub use debug_write::{DebugWriteJournal, DebugWriteOp};
pub use engine::QueryEngine;
pub use error::{QueryError, QueryResult};
pub use inspector::{ColumnSummary, InspectorConsole, InspectorPage, InspectorRow, TableSummary};
pub use sql_interface::{PreparedSql, SqlExecutionContext};
pub use typed_access::{
    BatchColumnReader, BooleanSlice, ColumnKind, ColumnView, F64Slice, I64Slice, RowCursor,
    RowLookupView, StringSlice, TypedColumnAccess,
};
pub use unified_read::{
    InMemoryReadSource, ReadRequest, ReadResponse, TableReadView, UnifiedReadSource,
};
