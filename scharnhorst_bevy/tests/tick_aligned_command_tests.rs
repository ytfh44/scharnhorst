//! Tick-Aligned Command Consumption Tests
//!
//! Tests for (Tick-Aligned Command Consumption) requirement.
//!
//! SPEC References:
//! - openspec/changes/bevy-arrow-grand-strategy-engine/specs/bevy-bridge/spec.md
//! - openspec/changes/bevy-arrow-grand-strategy-engine/specs/sim-scheduler/spec.md

use scharnhorst_arrow_store::ArrowStore;
use scharnhorst_bevy::{
    BevyBridgeError, BevyBridgeResult, CommandBatch, CommandBufferConsumer, CommandSource,
    InputCommandBuffer, RefreshHandlerConfig, SnapshotRefreshHandler, ViewModel,
};
use scharnhorst_core::{RowId, Tick};
use scharnhorst_journal::command::{Command, CommandEnvelope};
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_schema::SchemaRegistry;
use std::sync::Arc;

// ===================================================================
// 1. Basic Tick-Aligned Tests
// ===================================================================

/// Test: Commands are consumed at tick boundaries
///
/// Scenario: Commands buffered during tick N should be consumed
/// at the start of tick N+1.
#[test]
fn commands_consumed_at_tick_boundary() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Push commands during tick 1
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"unit": 1}),
    })?;
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"unit": 2}),
    })?;

    assert_eq!(buffer.len()?, 2);

    // Prepare and consume at tick 2 boundary
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;

    assert_eq!(batch.tick, Tick(2));
    assert_eq!(batch.commands.len(), 2);
    assert!(buffer.is_empty()?);
    assert_eq!(buffer.last_consumed_tick()?, Tick(2));

    Ok(())
}

/// Test: Commands accumulate within a tick
///
/// Scenario: Multiple commands pushed during the same tick
/// should all be available at the next tick boundary.
#[test]
fn commands_accumulate_within_tick() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(5))?;

    // Push multiple commands during tick 5
    for i in 0..5 {
        buffer.push(Command::Raw {
            domain: "action".to_owned(),
            payload: serde_json::json!({"index": i}),
        })?;
    }

    assert_eq!(buffer.len()?, 5);
    assert_eq!(buffer.pending_count()?, 5);

    // All commands should be available at tick 6
    buffer.prepare_for_tick(Tick(6))?;
    let batch = buffer.drain_commands(Tick(6))?;

    assert_eq!(batch.commands.len(), 5);

    Ok(())
}

/// Test: Commands are consumed in FIFO order
///
/// Scenario: Commands should be returned in the order they were pushed.
#[test]
fn commands_consumed_in_fifo_order() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Push commands in specific order
    let commands = vec![
        Command::Raw {
            domain: "first".to_owned(),
            payload: serde_json::json!({"order": 1}),
        },
        Command::Raw {
            domain: "second".to_owned(),
            payload: serde_json::json!({"order": 2}),
        },
        Command::Raw {
            domain: "third".to_owned(),
            payload: serde_json::json!({"order": 3}),
        },
    ];

    for cmd in &commands {
        buffer.push(cmd.clone())?;
    }

    // Consume and verify order
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;

    assert_eq!(batch.commands.len(), 3);
    assert_eq!(batch.commands[0].source, "player_0");
    assert_eq!(batch.commands[1].source, "player_0");
    assert_eq!(batch.commands[2].source, "player_0");

    // Verify payload order
    for (i, env) in batch.commands.iter().enumerate() {
        if let Command::Raw { payload, .. } = &env.command {
            let order = payload.get("order").and_then(|v| v.as_i64()).unwrap_or(-1);
            assert_eq!(order as usize, i + 1);
        }
    }

    Ok(())
}

// ===================================================================
// 2. Multiple Clicks Within Tick Scenario (SPEC )
// ===================================================================

/// Test: Multiple clicks within one tick are all captured
///
/// SPEC Scenario: Player clicks "Move" three times between two tick boundaries
/// EXPECTED: Bridge stores all three commands; scheduler pulls all three at tick start
#[test]
fn multiple_clicks_within_tick_all_captured() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Simulate player clicking "Move" three times within tick 1
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"target": "province_A", "click": 1}),
    })?;
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"target": "province_B", "click": 2}),
    })?;
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"target": "province_C", "click": 3}),
    })?;

    // Verify all three commands are buffered
    assert_eq!(buffer.len()?, 3);

    // Scheduler pulls all three at tick 2 start
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.consume_commands(Tick(2))?;

    assert_eq!(batch.commands.len(), 3);
    assert!(buffer.is_empty()?);

    // Verify all commands are "move" commands
    for env in &batch.commands {
        if let Command::Raw { domain, .. } = &env.command {
            assert_eq!(domain, "move");
        }
    }

    Ok(())
}

/// Test: Commands from multiple players are tracked separately
///
/// Scenario: Different players submit commands that should be identifiable.
#[test]
fn multiple_player_commands_tracked() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Player 1 commands
    buffer.submit_player_command(
        CommandSource::Player { player_id: 1 },
        Command::Raw {
            domain: "declare_war".to_owned(),
            payload: serde_json::json!({"target": "nation_B"}),
        },
    )?;

    // Player 2 commands
    buffer.submit_player_command(
        CommandSource::Player { player_id: 2 },
        Command::Raw {
            domain: "move_army".to_owned(),
            payload: serde_json::json!({"from": "X", "to": "Y"}),
        },
    )?;

    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;

    assert_eq!(batch.commands.len(), 2);

    // Verify sources are preserved
    let sources: Vec<_> = batch.commands.iter().map(|e| e.source.clone()).collect();
    assert!(sources.contains(&"player_1".to_string()));
    assert!(sources.contains(&"player_2".to_string()));

    Ok(())
}

// ===================================================================
// 3. Sim-Scheduler Integration Tests
// ===================================================================

/// Test: Scheduler consumes commands at tick start
///
/// SPEC Scenario: Network input arrives mid-tick
/// EXPECTED: Bridge buffers it; scheduler picks it up at next tick start
#[test]
fn scheduler_consumes_commands_at_tick_start() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Simulate network input arriving at 50ms into a 200ms tick
    buffer.push(Command::Raw {
        domain: "MoveArmy".to_owned(),
        payload: serde_json::json!({
            "army_id": 123,
            "destination": "province_45",
            "arrival_time": "50ms"
        }),
    })?;

    assert_eq!(buffer.len()?, 1);

    // Scheduler consumes at tick 2 start
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.consume_commands(Tick(2))?;

    assert_eq!(batch.commands.len(), 1);
    assert_eq!(batch.tick, Tick(2));

    // Verify command details
    let env = &batch.commands[0];
    if let Command::Raw { domain, payload } = &env.command {
        assert_eq!(domain, "MoveArmy");
        assert_eq!(payload.get("army_id").and_then(|v| v.as_i64()), Some(123));
    }

    Ok(())
}

/// Test: CommandBufferConsumer trait implementation
///
/// Scenario: Scheduler uses the trait to consume commands.
#[test]
fn command_buffer_consumer_trait_works() -> BevyBridgeResult<()> {
    // Need to set tick through concrete type first
    let concrete = InputCommandBuffer::new();
    concrete.set_tick(Tick(1))?;
    concrete.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // Use the trait method
    let batch = concrete.consume_commands(Tick(2))?;

    assert_eq!(batch.commands.len(), 1);
    assert_eq!(batch.tick, Tick(2));

    Ok(())
}

/// Test: Peek pending commands without consuming
///
/// Scenario: Scheduler or debug tools need to inspect pending commands.
#[test]
fn peek_pending_commands() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    buffer.push(Command::Raw {
        domain: "action1".to_owned(),
        payload: serde_json::json!({"id": 1}),
    })?;
    buffer.push(Command::Raw {
        domain: "action2".to_owned(),
        payload: serde_json::json!({"id": 2}),
    })?;

    // Peek without consuming
    let pending = buffer.peek_pending()?;
    assert_eq!(pending.len(), 2);

    // Buffer should still have commands
    assert_eq!(buffer.len()?, 2);

    // Now consume
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;
    assert_eq!(batch.commands.len(), 2);

    Ok(())
}

// ===================================================================
// 4. Command Source Tests (Player Commands Only)
// ===================================================================

/// Test: Player commands are accepted
///
/// SPEC: Bevy bridge SHALL ONLY buffer player-originated commands.
#[test]
fn player_commands_accepted() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Player command via source enum
    let player_source = CommandSource::Player { player_id: 42 };
    buffer.push_with_source(
        player_source,
        Command::Raw {
            domain: "move".to_owned(),
            payload: serde_json::Value::Null,
        },
    )?;

    // Player command via string
    buffer.submit_player_command(
        CommandSource::Player { player_id: 99 },
        Command::Raw {
            domain: "attack".to_owned(),
            payload: serde_json::Value::Null,
        },
    )?;

    assert_eq!(buffer.len()?, 2);

    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;
    assert_eq!(batch.commands.len(), 2);

    Ok(())
}

/// Test: AI commands are rejected
///
/// SPEC Scenario: AI declares war
/// EXPECTED: AI calls journal_system.submit directly, bypassing bridge buffer
/// Bridge should reject AI commands.
#[test]
fn ai_commands_rejected() {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1)).unwrap();

    let ai_source = CommandSource::Ai {
        ai_id: "general_1".to_string(),
    };
    let cmd = Command::Raw {
        domain: "declare_war".to_owned(),
        payload: serde_json::json!({
            "aggressor": RowId::new(1).0,
            "defender": RowId::new(2).0
        }),
    };

    let result = buffer.push_with_source(ai_source, cmd);
    assert!(
        matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)),
        "AI commands should be rejected by bridge"
    );
}

/// Test: Internal commands are rejected
///
/// SPEC: Internal simulation commands must bypass the bridge.
#[test]
fn internal_commands_rejected() {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1)).unwrap();

    let internal_source = CommandSource::Internal {
        system: "economy".to_string(),
    };
    let cmd = Command::UpdateColumn {
        table: "provinces".to_owned(),
        row: RowId::new(1),
        column: "gdp".to_owned(),
        value: serde_json::json!(1000),
    };

    let result = buffer.push_with_source(internal_source, cmd);
    assert!(
        matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)),
        "Internal commands should be rejected by bridge"
    );
}

/// Test: push_ai method always rejects
///
/// Convenience method for AI commands should always fail.
#[test]
fn push_ai_always_rejects() {
    let buffer = InputCommandBuffer::new();
    let cmd = Command::Raw {
        domain: "war".to_owned(),
        payload: serde_json::Value::Null,
    };

    let result = buffer.push_ai(cmd);
    assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
}

/// Test: AI command via string prefix rejected
///
/// String sources starting with "ai" should be rejected.
#[test]
fn ai_string_prefix_rejected() {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1)).unwrap();

    let cmd = Command::Raw {
        domain: "war".to_owned(),
        payload: serde_json::Value::Null,
    };

    let result = buffer.submit_player_command(
        CommandSource::Ai {
            ai_id: "general".to_string(),
        },
        cmd.clone(),
    );
    assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));

    let result = buffer.submit_player_command(
        CommandSource::Internal {
            system: "system".to_string(),
        },
        cmd,
    );
    assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
}

// ===================================================================
// 5. Tick Boundary Validation Tests
// ===================================================================

/// Test: Tick monotonicity - cannot go backwards
///
/// Scenario: prepare_for_tick with a lower tick should fail.
#[test]
fn tick_cannot_go_backwards() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    // Prepare for tick 5
    buffer.prepare_for_tick(Tick(5))?;

    // Trying to prepare for tick 3 should fail
    let result = buffer.prepare_for_tick(Tick(3));
    assert!(
        matches!(result, Err(BevyBridgeError::TickAlignmentError { expected, actual, .. }) if expected == Tick(5) && actual == Tick(3)),
        "Should fail when trying to go backwards: {:?}",
        result
    );

    Ok(())
}

/// Test: Tick monotonicity - same tick is allowed
///
/// Preparing for the same tick multiple times should be allowed.
#[test]
fn same_tick_allowed() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    buffer.prepare_for_tick(Tick(5))?;
    buffer.prepare_for_tick(Tick(5))?; // Same tick again

    assert_eq!(buffer.current_tick()?, Tick(5));

    Ok(())
}

/// Test: prepare_for_tick must be called before drain_commands
///
/// Scenario: drain_commands without prepare should fail.
#[test]
fn drain_requires_prepare() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // drain_commands without prepare_for_tick should fail
    let result = buffer.drain_commands(Tick(1));
    let is_tick_alignment_error = matches!(
        &result,
        Err(BevyBridgeError::TickAlignmentError { ref reason, .. })
            if reason.contains("prepare_for_tick")
    );
    assert!(
        is_tick_alignment_error,
        "Should fail when prepare_for_tick not called: {:?}",
        result
    );

    Ok(())
}

/// Test: Tick parameter must match prepared tick
///
/// Scenario: drain_commands with different tick than prepare_for_tick.
#[test]
fn drain_tick_must_match_prepare() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // Prepare for tick 5
    buffer.prepare_for_tick(Tick(5))?;

    // Try to drain with tick 6 - should fail
    let result = buffer.drain_commands(Tick(6));
    assert!(
        matches!(result, Err(BevyBridgeError::TickAlignmentError { expected, actual, .. }) if expected == Tick(5) && actual == Tick(6)),
        "Should fail when tick doesn't match: {:?}",
        result
    );

    Ok(())
}

/// Test: is_prepared tracks preparation state
///
/// Scenario: Verify the preparation flag is correctly managed.
#[test]
fn preparation_state_tracking() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    // Initially not prepared
    assert!(!buffer.is_prepared()?);

    // After prepare_for_tick
    buffer.prepare_for_tick(Tick(5))?;
    assert!(buffer.is_prepared()?);

    // After drain, should be reset
    buffer.drain_commands(Tick(5))?;
    assert!(!buffer.is_prepared()?);

    Ok(())
}

/// Test: last_consumed_tick tracking
///
/// Scenario: Verify the last consumed tick is updated correctly.
#[test]
fn last_consumed_tick_tracking() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // Initially zero
    assert_eq!(buffer.last_consumed_tick()?, Tick::ZERO);

    // Push and consume at tick 5
    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;
    buffer.prepare_for_tick(Tick(5))?;
    buffer.drain_commands(Tick(5))?;

    assert_eq!(buffer.last_consumed_tick()?, Tick(5));

    // Push and consume at tick 10
    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;
    buffer.prepare_for_tick(Tick(10))?;
    buffer.drain_commands(Tick(10))?;

    assert_eq!(buffer.last_consumed_tick()?, Tick(10));

    Ok(())
}

// ===================================================================
// 6. Error Scenario Tests
// ===================================================================

/// Test: Invalid tick sequence returns error
///
/// Scenario: Attempting to prepare for a tick before current.
#[test]
fn invalid_tick_sequence_error() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    buffer.prepare_for_tick(Tick(10))?;

    let result = buffer.prepare_for_tick(Tick(5));
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(
        matches!(err, BevyBridgeError::TickAlignmentError { .. }),
        "Expected TickAlignmentError, got {:?}",
        err
    );

    Ok(())
}

/// Test: Drain without prepare returns error
///
/// Scenario: Calling drain_commands without calling prepare_for_tick first.
#[test]
fn drain_without_prepare_error() {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1)).unwrap();

    buffer
        .push(Command::Raw {
            domain: "test".to_owned(),
            payload: serde_json::Value::Null,
        })
        .unwrap();

    let result = buffer.drain_commands(Tick(1));
    assert!(matches!(
        result,
        Err(BevyBridgeError::TickAlignmentError { .. })
    ));
}

/// Test: AI command via push_with_source returns error
///
/// Scenario: Attempting to push an AI command through the bridge.
#[test]
fn ai_command_via_source_error() {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1)).unwrap();

    let result = buffer.push_with_source(
        CommandSource::Ai {
            ai_id: "test".to_string(),
        },
        Command::Raw {
            domain: "war".to_owned(),
            payload: serde_json::Value::Null,
        },
    );

    assert!(matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)));
}

/// Test: CommandBatch empty state
///
/// Scenario: Verify CommandBatch correctly reports empty state.
#[test]
fn command_batch_empty_state() {
    let empty_batch = CommandBatch::new(Tick(1));
    assert!(empty_batch.is_empty());
    assert_eq!(empty_batch.commands.len(), 0);

    let commands = vec![CommandEnvelope::new(
        Tick(1),
        "player_1",
        Command::Raw {
            domain: "test".to_owned(),
            payload: serde_json::Value::Null,
        },
    )];
    let non_empty = CommandBatch::with_commands(Tick(1), commands);
    assert!(!non_empty.is_empty());
    assert_eq!(non_empty.commands.len(), 1);
}

// ===================================================================
// 7. REFRESH_SIGNAL Integration Tests
// ===================================================================

/// Test: Refresh handler initial state
///
/// Scenario: Verify handler starts in correct state.
#[test]
fn refresh_handler_initial_state() -> BevyBridgeResult<()> {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm, qe, store);
    assert!(!handler.should_refresh()?);
    assert_eq!(handler.current_generation()?, 0);
    Ok(())
}

/// Test: Refresh signal sets should_refresh flag
///
/// Scenario: on_refresh_signal should set the flag.
#[test]
fn refresh_signal_sets_flag() -> BevyBridgeResult<()> {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm, qe, store);

    let _ = handler.on_refresh_signal();
    assert!(handler.should_refresh()?);
    Ok(())
}

#[test]
fn refresh_snapshot_updates_generation() -> BevyBridgeResult<()> {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm.clone(), qe, store);

    // Initial state
    assert_eq!(handler.current_generation()?, 0);

    // Signal and refresh
    let _ = handler.on_refresh_signal();
    let snapshot = scharnhorst_arrow_store::WorldSnapshot::new(Tick(5));
    handler.refresh_snapshot(snapshot)?;

    // After refresh
    assert!(!handler.should_refresh()?);
    assert_eq!(handler.current_generation()?, 1);
    assert_eq!(vm.generation()?, 1);
    assert_eq!(vm.latest_tick()?, Some(Tick(5)));

    Ok(())
}

/// Test: Multiple refresh cycles
///
/// Scenario: Multiple tick boundaries with refresh signals.
#[test]
fn multiple_refresh_cycles() -> BevyBridgeResult<()> {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm.clone(), qe, store);

    for i in 1..=3 {
        // Signal from scheduler
        let _ = handler.on_refresh_signal();
        assert!(handler.should_refresh()?);

        // Bridge refreshes snapshot
        let snapshot = scharnhorst_arrow_store::WorldSnapshot::new(Tick(i));
        handler.refresh_snapshot(snapshot)?;

        assert!(!handler.should_refresh()?);
        assert_eq!(handler.current_generation()?, i);
    }

    assert_eq!(vm.generation()?, 3);
    assert_eq!(vm.latest_tick()?, Some(Tick(3)));

    Ok(())
}

/// Test: Refresh handler config
///
/// Scenario: Verify config can be created.
#[test]
fn refresh_handler_config_creation() {
    let config = RefreshHandlerConfig::new("bevy_bridge");
    assert_eq!(config.consumer_name, "bevy_bridge");
}

// ===================================================================
// 8. Tick Lifecycle Integration Tests
// ===================================================================

/// Test: Complete tick lifecycle
///
/// Scenario: Simulate a complete tick with command consumption.
#[test]
fn complete_tick_lifecycle() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    // T+0: Set initial tick
    buffer.set_tick(Tick(0))?;

    // During tick 0: Player submits commands
    buffer.push(Command::Raw {
        domain: "build".to_owned(),
        payload: serde_json::json!({"building": "factory"}),
    })?;
    buffer.push(Command::Raw {
        domain: "recruit".to_owned(),
        payload: serde_json::json!({"unit": "infantry", "count": 100}),
    })?;

    assert_eq!(buffer.len()?, 2);

    // T+1 Start: Scheduler prepares and consumes
    buffer.prepare_for_tick(Tick(1))?;
    let batch = buffer.drain_commands(Tick(1))?;

    assert_eq!(batch.commands.len(), 2);
    assert_eq!(batch.tick, Tick(1));
    assert!(buffer.is_empty()?);

    // During tick 1: More commands
    buffer.push(Command::Raw {
        domain: "move".to_owned(),
        payload: serde_json::json!({"unit": 1}),
    })?;

    // T+2 Start: Scheduler consumes
    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;
    assert_eq!(batch.commands.len(), 1);

    Ok(())
}

#[test]
/// Test: Empty batch when no commands
///
/// Scenario: Tick boundary with no pending commands.
fn empty_batch_when_no_commands() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    // No commands pushed

    buffer.prepare_for_tick(Tick(2))?;
    let batch = buffer.drain_commands(Tick(2))?;

    assert!(batch.is_empty());
    assert_eq!(batch.commands.len(), 0);

    Ok(())
}

/// Test: Commands are cleared after drain
///
/// Scenario: After drain_commands, buffer should be empty.
#[test]
fn commands_cleared_after_drain() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    buffer.prepare_for_tick(Tick(2))?;
    buffer.drain_commands(Tick(2))?;

    // Buffer should be empty
    assert!(buffer.is_empty()?);
    assert_eq!(buffer.len()?, 0);
    assert_eq!(buffer.peek()?, None);

    Ok(())
}

// ===================================================================
// 9. Command Envelope Tests
// ===================================================================

/// Test: CommandEnvelope creation
///
/// Scenario: Verify envelope is created with correct fields.
#[test]
fn command_envelope_creation() {
    let cmd = Command::TransferControl {
        province_id: RowId::new(1),
        from_actor: RowId::new(2),
        to_actor: RowId::new(3),
    };

    let envelope = CommandEnvelope::new(Tick(42), "player_5", cmd.clone());

    assert_eq!(envelope.tick, Tick(42));
    assert_eq!(envelope.source, "player_5");
    assert_eq!(envelope.command, cmd);
}

/// Test: CommandSource player variant
///
/// Scenario: Verify player source properties.
#[test]
fn command_source_player() {
    let source = CommandSource::Player { player_id: 123 };

    assert!(source.is_player());
    assert!(!source.is_ai());
    assert!(!source.is_internal());
    assert_eq!(source.to_source_string(), "player_123");
}

/// Test: CommandSource AI variant
///
/// Scenario: Verify AI source properties.
#[test]
fn command_source_ai() {
    let source = CommandSource::Ai {
        ai_id: "general_napoleon".to_string(),
    };

    assert!(!source.is_player());
    assert!(source.is_ai());
    assert!(!source.is_internal());
    assert_eq!(source.to_source_string(), "ai_general_napoleon");
}

/// Test: CommandSource internal variant
///
/// Scenario: Verify internal source properties.
#[test]
fn command_source_internal() {
    let source = CommandSource::Internal {
        system: "economy_calculator".to_string(),
    };

    assert!(!source.is_player());
    assert!(!source.is_ai());
    assert!(source.is_internal());
    assert_eq!(source.to_source_string(), "internal_economy_calculator");
}

// ===================================================================
// 10. Buffer State Management Tests
// ===================================================================

/// Test: Clear removes all pending commands
///
/// Scenario: Clear buffer mid-tick.
#[test]
fn clear_removes_all_commands() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;
    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    assert_eq!(buffer.len()?, 2);

    buffer.clear()?;

    assert!(buffer.is_empty()?);
    assert_eq!(buffer.len()?, 0);

    Ok(())
}

/// Test: Peek returns front command without removal
///
/// Scenario: Inspect next command without consuming.
#[test]
fn peek_returns_front_without_removal() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(1))?;

    let cmd1 = Command::Raw {
        domain: "first".to_owned(),
        payload: serde_json::json!({"id": 1}),
    };
    let cmd2 = Command::Raw {
        domain: "second".to_owned(),
        payload: serde_json::json!({"id": 2}),
    };

    buffer.push(cmd1.clone())?;
    buffer.push(cmd2.clone())?;

    // Peek should return first command
    let peeked = buffer.peek()?;
    assert!(peeked.is_some());
    assert_eq!(peeked.unwrap().command, cmd1);

    // Buffer should still have both commands
    assert_eq!(buffer.len()?, 2);

    Ok(())
}

/// Test: Player ID management
///
/// Scenario: Set and get player ID.
#[test]
fn player_id_management() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new().with_player_id(42)?;
    assert_eq!(buffer.player_id()?, 42);

    buffer.set_player_id(99)?;
    assert_eq!(buffer.player_id()?, 99);

    // Commands should use current player ID
    buffer.set_tick(Tick(1))?;
    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    let drained = buffer.drain()?;
    assert_eq!(drained[0].source, "player_99");

    Ok(())
}

/// Test: Current tick tracking
///
/// Scenario: Set and get current tick.
#[test]
fn current_tick_tracking() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();

    // Default is ZERO
    assert_eq!(buffer.current_tick()?, Tick::ZERO);

    buffer.set_tick(Tick(100))?;
    assert_eq!(buffer.current_tick()?, Tick(100));

    Ok(())
}

/// Test: Commands stamped with current tick
///
/// Scenario: Commands should be stamped with the tick at push time.
#[test]
fn commands_stamped_with_push_tick() -> BevyBridgeResult<()> {
    let buffer = InputCommandBuffer::new();
    buffer.set_tick(Tick(50))?;

    buffer.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // Even though we consume at tick 51, envelope should have tick 50
    buffer.prepare_for_tick(Tick(51))?;
    let batch = buffer.drain_commands(Tick(51))?;

    assert_eq!(batch.commands[0].tick, Tick(50));

    Ok(())
}
