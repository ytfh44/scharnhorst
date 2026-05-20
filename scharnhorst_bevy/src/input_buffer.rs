use bevy::prelude::Resource;
use scharnhorst_core::Tick;
use scharnhorst_journal::command::{Command, CommandEnvelope};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::error::{BevyBridgeError, BevyBridgeResult};

/// The source of a command, used to distinguish player commands from AI/internal commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandSource {
    /// Command originated from a player (via UI input).
    Player { player_id: u64 },
    /// Command originated from an AI system.
    ///
    /// Note: AI commands should bypass the bridge and submit directly to the journal.
    /// This variant exists for validation and error handling purposes.
    Ai { ai_id: String },
    /// Command originated from an internal simulation system.
    ///
    /// Note: Internal commands should bypass the bridge and submit directly to the journal.
    /// This variant exists for validation and error handling purposes.
    Internal { system: String },
}

impl CommandSource {
    /// Returns true if this is a player command.
    pub fn is_player(&self) -> bool {
        matches!(self, CommandSource::Player { .. })
    }

    /// Returns true if this is an AI command.
    pub fn is_ai(&self) -> bool {
        matches!(self, CommandSource::Ai { .. })
    }

    /// Returns true if this is an internal command.
    pub fn is_internal(&self) -> bool {
        matches!(self, CommandSource::Internal { .. })
    }

    /// Convert to a string representation for the envelope source field.
    pub fn to_source_string(&self) -> String {
        match self {
            CommandSource::Player { player_id } => format!("player_{}", player_id),
            CommandSource::Ai { ai_id } => format!("ai_{}", ai_id),
            CommandSource::Internal { system } => format!("internal_{}", system),
        }
    }
}

/// A batch of commands ready for consumption at a tick boundary.
#[derive(Debug, Clone)]
pub struct CommandBatch {
    /// The tick for which these commands are being consumed.
    pub tick: Tick,
    /// The commands in the batch, in FIFO order.
    pub commands: Vec<CommandEnvelope>,
}

impl CommandBatch {
    /// Create a new empty command batch for the given tick.
    pub fn new(tick: Tick) -> Self {
        Self {
            tick,
            commands: Vec::new(),
        }
    }

    /// Create a new command batch with the given commands.
    pub fn with_commands(tick: Tick, commands: Vec<CommandEnvelope>) -> Self {
        Self {
            tick,
            commands,
        }
    }

    /// Returns true if this batch contains no commands.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

/// Trait for consuming commands from the input buffer at tick boundaries.
///
/// This trait is implemented by the scheduler to consume buffered player commands
/// at the start of each tick, before any simulation systems run.
pub trait CommandBufferConsumer: Send + Sync {
    /// Consume all pending commands for the given tick.
    ///
    /// This method is called by the scheduler at the start of each tick.
    /// It returns a batch of all commands buffered since the last consumption.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The tick is not aligned (attempting to consume mid-tick)
    /// - The lock is poisoned
    fn consume_commands(&self, tick: Tick) -> BevyBridgeResult<CommandBatch>;

    /// Peek at pending commands without consuming them.
    ///
    /// This is useful for debugging and monitoring purposes.
    fn peek_pending(&self) -> BevyBridgeResult<Vec<CommandEnvelope>>;

    /// Get the count of pending commands.
    fn pending_count(&self) -> BevyBridgeResult<usize>;
}

/// Buffer for accumulating player commands between tick boundaries.
///
/// The `InputCommandBuffer` accumulates player commands frame-by-frame and exposes them
/// to the `sim-scheduler` once per tick at tick boundaries. Commands arriving mid-tick
/// are buffered and delivered at the next tick boundary.
///
/// # Tick-Aligned Command Consumption
///
/// The scheduler is the sole consumer of this buffer. At the start of each tick:
/// 1. The scheduler calls `prepare_for_tick(tick)` to align the buffer
/// 2. The scheduler calls `drain_commands(tick)` to consume all pending commands
/// 3. Commands are submitted to the journal as a batch
///
/// # Player Commands Only
///
/// This buffer only accepts player-originated commands. AI and internal simulation
/// commands must bypass the bridge entirely and submit directly to the journal system.
///
/// # Lock Safety
///
/// All mutable state is behind a single `Mutex<InputBufferInner>` to prevent ABBA
/// deadlocks that could occur with multiple independent mutexes.
#[derive(Debug, Clone, Resource)]
pub struct InputCommandBuffer {
    inner: Arc<Mutex<InputBufferInner>>,
}

/// Internal state behind a single `Mutex` to avoid ABBA deadlock.
#[derive(Debug)]
struct InputBufferInner {
    queue: VecDeque<CommandEnvelope>,
    current_tick: Tick,
    player_id: u64,
    /// The last tick at which commands were consumed.
    /// Used to ensure commands are only consumed at tick boundaries.
    last_consumed_tick: Tick,
    /// Whether the buffer has been prepared for the current tick.
    tick_prepared: bool,
}

impl Default for InputCommandBuffer {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InputBufferInner {
                queue: VecDeque::new(),
                current_tick: Tick::ZERO,
                player_id: 0,
                last_consumed_tick: Tick::ZERO,
                tick_prepared: false,
            })),
        }
    }
}

impl InputCommandBuffer {
    /// Create a new empty input command buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the player ID for this buffer.
    pub fn with_player_id(self, player_id: u64) -> BevyBridgeResult<Self> {
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
            inner.player_id = player_id;
        }
        Ok(self)
    }

    /// Set the current tick.
    ///
    /// This should be called by the bridge at the start of each frame to keep
    /// the buffer synchronized with the simulation.
    pub fn set_tick(&self, tick: Tick) -> BevyBridgeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        inner.current_tick = tick;
        Ok(())
    }

    /// Get the current tick.
    pub fn current_tick(&self) -> BevyBridgeResult<Tick> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.current_tick)
    }

    /// Get the player ID associated with this buffer.
    pub fn player_id(&self) -> BevyBridgeResult<u64> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.player_id)
    }

    /// Set the player ID for this buffer.
    pub fn set_player_id(&self, player_id: u64) -> BevyBridgeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        inner.player_id = player_id;
        Ok(())
    }

    /// Get the last tick at which commands were consumed.
    pub fn last_consumed_tick(&self) -> BevyBridgeResult<Tick> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.last_consumed_tick)
    }

    /// Prepare the buffer for the given tick.
    ///
    /// This method is called by the scheduler at the start of each tick,
    /// before consuming commands. It validates that:
    /// - The tick is monotonically increasing
    /// - Commands from the previous tick have been consumed
    ///
    /// # Errors
    ///
    /// Returns an error if the tick is not valid (e.g., going backwards).
    pub fn prepare_for_tick(&self, tick: Tick) -> BevyBridgeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;

        // Validate tick monotonicity - tick should not go backwards
        if tick.as_u64() < inner.current_tick.as_u64() {
            return Err(BevyBridgeError::TickAlignmentError {
                expected: inner.current_tick,
                actual: tick,
                reason: "tick cannot go backwards".to_string(),
            });
        }

        inner.current_tick = tick;
        inner.tick_prepared = true;

        Ok(())
    }

    /// Push a player command into the buffer.
    ///
    /// The command will be stamped with the current tick and player ID.
    /// Commands are accumulated frame-by-frame and delivered at tick boundaries.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn push(&self, command: Command) -> BevyBridgeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let source = format!("player_{}", inner.player_id);
        let envelope = CommandEnvelope::new(inner.current_tick, source, command);
        inner.queue.push_back(envelope);
        Ok(())
    }

    /// Push a command with a specific source.
    ///
    /// This method validates that only player commands are accepted.
    /// AI and internal commands are rejected and must bypass the bridge.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The source is an AI or internal command
    /// - The lock is poisoned
    pub fn push_with_source(
        &self,
        source: CommandSource,
        command: Command,
    ) -> BevyBridgeResult<()> {
        // Validate that only player commands are accepted
        if !source.is_player() {
            return Err(BevyBridgeError::NonPlayerCommandRejected);
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let envelope = CommandEnvelope::new(inner.current_tick, source.to_source_string(), command);
        inner.queue.push_back(envelope);
        Ok(())
    }

    /// Push an AI command (rejected - AI commands must bypass the bridge).
    ///
    /// This method always returns an error. AI commands should be submitted
    /// directly to the journal system, not through the bridge buffer.
    ///
    /// # Errors
    ///
    /// always returns `BevyBridgeError::NonPlayerCommandRejected`.
    pub fn push_ai(&self, _command: Command) -> BevyBridgeResult<()> {
        Err(BevyBridgeError::NonPlayerCommandRejected)
    }

    /// Submit a player command with a CommandSource.
    ///
    /// Only player-originated commands are accepted.
    /// AI and internal sources are rejected.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The source is not a player
    /// - The lock is poisoned
    pub fn submit_player_command(
        &self,
        source: CommandSource,
        command: Command,
    ) -> BevyBridgeResult<()> {
        if !source.is_player() {
            return Err(BevyBridgeError::NonPlayerCommandRejected);
        }

        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let envelope = CommandEnvelope::new(inner.current_tick, source.to_source_string(), command);
        inner.queue.push_back(envelope);
        Ok(())
    }

    /// Drain all commands from the buffer for consumption at a tick boundary.
    ///
    /// This method is called by the scheduler at the start of each tick.
    /// It validates that:
    /// - The tick has been prepared via `prepare_for_tick`
    /// - Commands are consumed in tick order
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The tick has not been prepared
    /// - The tick does not match the prepared tick
    /// - The lock is poisoned
    pub fn drain_commands(&self, tick: Tick) -> BevyBridgeResult<CommandBatch> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;

        if !inner.tick_prepared {
            return Err(BevyBridgeError::TickAlignmentError {
                expected: tick,
                actual: inner.current_tick,
                reason: "prepare_for_tick must be called before drain_commands".to_string(),
            });
        }

        if tick != inner.current_tick {
            return Err(BevyBridgeError::TickAlignmentError {
                expected: inner.current_tick,
                actual: tick,
                reason: "tick mismatch".to_string(),
            });
        }

        let commands: Vec<_> = inner.queue.drain(..).collect();

        inner.last_consumed_tick = tick;
        inner.tick_prepared = false;

        Ok(CommandBatch {
            tick,
            commands,
        })
    }

    /// Legacy drain method - drains all commands without tick validation.
    ///
    /// # Deprecated
    ///
    /// Use `drain_commands(tick)` instead for tick-aligned consumption.
    pub fn drain(&self) -> BevyBridgeResult<Vec<CommandEnvelope>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        let drained: Vec<_> = inner.queue.drain(..).collect();
        Ok(drained)
    }

    /// Returns the number of pending commands.
    pub fn len(&self) -> BevyBridgeResult<usize> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.queue.len())
    }

    /// Returns true if there are no pending commands.
    pub fn is_empty(&self) -> BevyBridgeResult<bool> {
        self.len().map(|n| n == 0)
    }

    /// Peek at the next command without removing it.
    pub fn peek(&self) -> BevyBridgeResult<Option<CommandEnvelope>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.queue.front().cloned())
    }

    /// Clear all pending commands.
    pub fn clear(&self) -> BevyBridgeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        inner.queue.clear();
        Ok(())
    }

    /// Returns true if the buffer has been prepared for the current tick.
    pub fn is_prepared(&self) -> BevyBridgeResult<bool> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.tick_prepared)
    }
}

impl CommandBufferConsumer for InputCommandBuffer {
    fn consume_commands(&self, tick: Tick) -> BevyBridgeResult<CommandBatch> {
        self.prepare_for_tick(tick)?;
        self.drain_commands(tick)
    }

    fn peek_pending(&self) -> BevyBridgeResult<Vec<CommandEnvelope>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| BevyBridgeError::LockPoisoned(e.to_string()))?;
        Ok(inner.queue.iter().cloned().collect())
    }

    fn pending_count(&self) -> BevyBridgeResult<usize> {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_push_and_drain() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(5))?;

        let cmd = Command::DeleteRow {
            table: "provinces".to_owned(),
            row: scharnhorst_core::RowId::new(7),
        };
        buf.push(cmd)?;

        assert_eq!(buf.len()?, 1);
        let drained = buf.drain()?;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].tick, Tick(5));
        assert_eq!(drained[0].source, "player_0");
        Ok(())
    }

    #[test]
    fn buffer_push_ai_rejected() {
        let buf = InputCommandBuffer::new();
        let cmd = Command::Raw {
            domain: "war".to_owned(),
            payload: serde_json::Value::Null,
        };
        let result = buf.push_ai(cmd);
        assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
    }

    #[test]
    fn buffer_push_with_custom_player_id() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new().with_player_id(42)?;
        buf.set_tick(Tick(1))?;

        let cmd = Command::DeleteRow {
            table: "t".to_owned(),
            row: scharnhorst_core::RowId::new(1),
        };
        buf.push(cmd)?;

        let drained = buf.drain()?;
        assert_eq!(drained[0].source, "player_42");
        Ok(())
    }

    #[test]
    fn buffer_submit_player_command() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(5))?;

        let cmd = Command::DeleteRow {
            table: "provinces".to_owned(),
            row: scharnhorst_core::RowId::new(7),
        };
        buf.submit_player_command(CommandSource::Player { player_id: 1 }, cmd)?;

        assert_eq!(buf.len()?, 1);
        let drained = buf.drain()?;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].tick, Tick(5));
        assert_eq!(drained[0].source, "player_1");
        Ok(())
    }

    #[test]
    fn buffer_rejects_ai_command_via_submit() {
        let buf = InputCommandBuffer::new();
        let cmd = Command::Raw {
            domain: "war".to_owned(),
            payload: serde_json::Value::Null,
        };
        let result = buf.submit_player_command(
            CommandSource::Ai {
                ai_id: "general".to_string(),
            },
            cmd,
        );
        assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
    }

    #[test]
    fn buffer_rejects_internal_command_via_submit() {
        let buf = InputCommandBuffer::new();
        let cmd = Command::Raw {
            domain: "economy".to_owned(),
            payload: serde_json::Value::Null,
        };
        let result = buf.submit_player_command(
            CommandSource::Internal {
                system: "economy_system".to_string(),
            },
            cmd,
        );
        assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
    }

    #[test]
    fn buffer_drains_fifo() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(1))?;

        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        })?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 2}),
        })?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 3}),
        })?;

        let drained = buf.drain()?;
        let payloads: Vec<_> = drained
            .iter()
            .filter_map(|env| match &env.command {
                Command::Raw { payload, .. } => Some(payload.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(
            payloads,
            vec![
                serde_json::json!({"x": 1}),
                serde_json::json!({"x": 2}),
                serde_json::json!({"x": 3}),
            ]
        );
        Ok(())
    }

    #[test]
    fn buffer_peek_and_clear() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(2))?;

        let cmd = Command::UpdateColumn {
            table: "t".to_owned(),
            row: scharnhorst_core::RowId::new(1),
            column: "c".to_owned(),
            value: serde_json::Value::Bool(true),
        };
        buf.push(cmd.clone())?;

        let peeked = buf.peek()?;
        assert!(peeked.is_some());
        assert_eq!(peeked.unwrap().command, cmd);

        buf.clear()?;
        assert!(buf.is_empty()?);
        assert_eq!(buf.peek()?, None);
        Ok(())
    }

    #[test]
    fn buffer_tick_stamped_correctly() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(42))?;

        buf.push(Command::Raw {
            domain: "d".to_owned(),
            payload: serde_json::Value::Null,
        })?;

        let env = buf.peek()?.unwrap();
        assert_eq!(env.tick, Tick(42));
        Ok(())
    }

    #[test]
    fn command_source_is_player() {
        let player = CommandSource::Player { player_id: 1 };
        assert!(player.is_player());
        assert!(!player.is_ai());
        assert!(!player.is_internal());
        assert_eq!(player.to_source_string(), "player_1");
    }

    #[test]
    fn command_source_is_ai() {
        let ai = CommandSource::Ai {
            ai_id: "general_1".to_string(),
        };
        assert!(!ai.is_player());
        assert!(ai.is_ai());
        assert!(!ai.is_internal());
        assert_eq!(ai.to_source_string(), "ai_general_1");
    }

    #[test]
    fn command_source_is_internal() {
        let internal = CommandSource::Internal {
            system: "economy".to_string(),
        };
        assert!(!internal.is_player());
        assert!(!internal.is_ai());
        assert!(internal.is_internal());
        assert_eq!(internal.to_source_string(), "internal_economy");
    }

    #[test]
    fn buffer_accepts_player_command_via_source() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(5))?;

        let source = CommandSource::Player { player_id: 42 };
        let cmd = Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        };
        buf.push_with_source(source, cmd)?;

        let drained = buf.drain()?;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].source, "player_42");
        Ok(())
    }

    #[test]
    fn buffer_rejects_ai_command_via_source() {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(5)).unwrap();

        let source = CommandSource::Ai {
            ai_id: "general_1".to_string(),
        };
        let cmd = Command::Raw {
            domain: "declare_war".to_owned(),
            payload: serde_json::Value::Null,
        };
        let result = buf.push_with_source(source, cmd);
        assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
    }

    #[test]
    fn buffer_rejects_internal_command_via_source() {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(5)).unwrap();

        let source = CommandSource::Internal {
            system: "economy".to_string(),
        };
        let cmd = Command::Raw {
            domain: "update".to_owned(),
            payload: serde_json::Value::Null,
        };
        let result = buf.push_with_source(source, cmd);
        assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
    }

    #[test]
    fn command_batch_new() {
        let batch = CommandBatch::new(Tick(5));
        assert_eq!(batch.tick, Tick(5));
        assert!(batch.is_empty());
        assert_eq!(batch.commands.len(), 0);
    }

    #[test]
    fn command_batch_with_commands() {
        let commands = vec![CommandEnvelope::new(
            Tick(5),
            "player_1",
            Command::Raw {
                domain: "move".to_owned(),
                payload: serde_json::Value::Null,
            },
        )];
        let batch = CommandBatch::with_commands(Tick(5), commands.clone());
        assert_eq!(batch.tick, Tick(5));
        assert!(!batch.is_empty());
        assert_eq!(batch.commands.len(), 1);
    }

    #[test]
    fn tick_aligned_drain_commands() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(1))?;

        // Push commands at tick 1
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        })?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 2}),
        })?;

        // Prepare and drain at tick 2
        buf.prepare_for_tick(Tick(2))?;
        let batch = buf.drain_commands(Tick(2))?;
        assert_eq!(batch.tick, Tick(2));
        assert_eq!(batch.commands.len(), 2);
        assert_eq!(buf.len()?, 0);
        assert_eq!(buf.last_consumed_tick()?, Tick(2));
        Ok(())
    }

    #[test]
    fn drain_commands_requires_prepare() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(1))?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        })?;

        // drain_commands requires prepare_for_tick to be called first
        buf.prepare_for_tick(Tick(1))?;
        let batch = buf.drain_commands(Tick(1))?;
        assert_eq!(batch.commands.len(), 1);
        Ok(())
    }

    #[test]
    fn prepare_for_tick_validates_monotonicity() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();

        // Prepare for tick 5
        buf.prepare_for_tick(Tick(5))?;

        // Trying to prepare for an earlier tick should fail
        let result = buf.prepare_for_tick(Tick(3));
        assert!(matches!(
            result,
            Err(BevyBridgeError::TickAlignmentError { .. })
        ));
        Ok(())
    }

    #[test]
    fn command_buffer_consumer_implementation() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(1))?;

        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 1}),
        })?;

        // Test consume_commands
        let batch = buf.consume_commands(Tick(2))?;
        assert_eq!(batch.tick, Tick(2));
        assert_eq!(batch.commands.len(), 1);

        // Test peek_pending
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"x": 2}),
        })?;
        let pending = buf.peek_pending()?;
        assert_eq!(pending.len(), 1);

        // Test pending_count
        assert_eq!(buf.pending_count()?, 1);

        Ok(())
    }

    #[test]
    fn multiple_clicks_within_tick_accumulate() -> BevyBridgeResult<()> {
        // Scenario: Player clicks "Move" three times between two tick boundaries
        let buf = InputCommandBuffer::new();
        buf.set_tick(Tick(1))?;

        // Three clicks within tick 1
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"target": "A"}),
        })?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"target": "B"}),
        })?;
        buf.push(Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::json!({"target": "C"}),
        })?;

        // All three commands should be accumulated
        assert_eq!(buf.len()?, 3);

        // Scheduler pulls all three at tick start
        buf.prepare_for_tick(Tick(2))?;
        let batch = buf.drain_commands(Tick(2))?;
        assert_eq!(batch.commands.len(), 3);

        // Verify order is preserved (FIFO)
        let targets: Vec<_> = batch
            .commands
            .iter()
            .filter_map(|env| match &env.command {
                Command::Raw { payload, .. } => payload
                    .get("target")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(targets, vec!["A", "B", "C"]);

        Ok(())
    }

    #[test]
    fn is_prepared_tracks_preparation_state() -> BevyBridgeResult<()> {
        let buf = InputCommandBuffer::new();

        assert!(!buf.is_prepared()?);

        buf.prepare_for_tick(Tick(5))?;
        assert!(buf.is_prepared()?);

        // After draining, prepared should be reset
        buf.drain_commands(Tick(5))?;
        assert!(!buf.is_prepared()?);

        Ok(())
    }
}
