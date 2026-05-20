//! scharnhorst_journal: deterministic journal / replay log.
//!
//! This crate provides the sole write entry point for all world state
//! mutations. It defines [`Command`] and [`Diff`] types, the [`Journal`]
//! struct, and the [`SaveJournal`] trait for incremental persistence.

pub mod command;
pub mod commit;
pub mod diff;
pub mod error;
pub mod journal;
pub mod save_journal;

// SQL parser module is only available in debug builds
#[cfg(debug_assertions)]
pub mod sql_parser;

pub use command::{Command, CommandEnvelope};
pub use commit::{CommitPhase, CommitRecord, CommitResult};
pub use diff::{Diff, DiffBatch};
pub use error::{JournalError, JournalResult};
pub use journal::Journal;
pub use save_journal::{InMemorySaveJournal, SaveJournal};

/// Debug-only write path that routes SQL `UPDATE`/`INSERT`/`DELETE`
/// statements into [`Diff`] objects.
#[cfg(debug_assertions)]
pub use journal::DebugWriteJournal;

// Re-export SQL parser types for debug builds
#[cfg(debug_assertions)]
pub use sql_parser::{ComparisonOp, SqlParser, SqlStatement, SqlValue, WhereCondition};
