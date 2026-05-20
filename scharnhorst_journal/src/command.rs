use scharnhorst_core::{RowId, Tick};
use serde::{Deserialize, Serialize};

/// An external or internal intent that mutates world state.
///
/// Commands are the only way to express mutations that originate
/// outside the simulation systems (player input, AI decisions, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Transfer control of a province from one actor to another.
    TransferControl {
        province_id: RowId,
        from_actor: RowId,
        to_actor: RowId,
    },
    /// Update a single column value for a specific row.
    UpdateColumn {
        table: String,
        row: RowId,
        column: String,
        /// JSON-encoded value.
        value: serde_json::Value,
    },
    /// Insert a new row into a table.
    InsertRow {
        table: String,
        /// JSON-encoded row values keyed by column name.
        values: serde_json::Map<String, serde_json::Value>,
    },
    /// Delete a row from a table.
    DeleteRow { table: String, row: RowId },
    /// A raw, domain-specific command encoded as JSON.
    Raw {
        domain: String,
        payload: serde_json::Value,
    },
}

/// Metadata wrapper that binds a [`Command`] to a specific tick and source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandEnvelope {
    /// The tick at which the command was received.
    pub tick: Tick,
    /// An opaque identifier for the source of the command (player id, system name, etc.).
    pub source: String,
    /// The command payload.
    pub command: Command,
}

impl CommandEnvelope {
    /// Create a new envelope for the given tick, source, and command.
    pub fn new(tick: Tick, source: impl Into<String>, command: Command) -> Self {
        Self {
            tick,
            source: source.into(),
            command,
        }
    }
}
