use scharnhorst_arrow_store::{ArrowStore, InitStore, WorldView};
use scharnhorst_bevy::{
    BevyBridgeError, BevyBridgeResult, CommandSource, EntityMaterializationRegistry,
    InputCommandBuffer, MaterializationConfig, MaterializationFilter, MaterializeRequest,
    NullSyncField, RefreshHandlerConfig, SnapshotRefreshHandler, SyncField, SyncState, ViewModel,
    ViewOf,
};
use scharnhorst_core::{RowId, Tick};
use scharnhorst_journal::command::Command;
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
use std::sync::Arc;

// ------------------------------------------------------------------
// ViewOf component tests
// ------------------------------------------------------------------

#[test]
fn view_of_stores_row_id_and_table_name() {
    let view = ViewOf::new(RowId::new(42), "provinces");
    assert_eq!(view.row_id(), RowId::new(42));
    assert_eq!(view.table_name(), "provinces");
}

#[test]
fn view_of_is_clone() {
    let a = ViewOf::new(RowId::new(1), "units");
    let b = a.clone();
    assert_eq!(a, b);
}

#[test]
fn view_of_default_generation_is_zero() {
    let view = ViewOf::new(RowId::new(1), "units");
    assert_eq!(view.generation(), 0);
}

#[test]
fn view_of_set_generation() {
    let mut view = ViewOf::new(RowId::new(1), "units");
    view.set_generation(5);
    assert_eq!(view.generation(), 5);
}

// ------------------------------------------------------------------
// EntityMaterializationRegistry tests
// ------------------------------------------------------------------

#[test]
fn registry_round_trip() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    let entity = bevy::prelude::Entity::from_raw_u32(7).expect("Entity index must be valid");

    reg.register("provinces", RowId::new(99), entity)?;
    let looked_up = reg.lookup("provinces", RowId::new(99))?;
    assert_eq!(looked_up, Some(entity));
    Ok(())
}

#[test]
fn registry_lookup_missing_returns_none() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    let result = reg.lookup("provinces", RowId::new(1))?;
    assert_eq!(result, None);
    Ok(())
}

#[test]
fn registry_unregister_removes_mapping() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    let entity = bevy::prelude::Entity::from_raw_u32(3).expect("Entity index must be valid");

    reg.register("actors", RowId::new(5), entity)?;
    let removed = reg.unregister("actors", RowId::new(5))?;
    assert_eq!(removed, Some(entity));

    let missing = reg.lookup("actors", RowId::new(5))?;
    assert_eq!(missing, None);
    Ok(())
}

#[test]
fn registry_len_and_is_empty() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    assert!(reg.is_empty()?);

    reg.register(
        "t",
        RowId::new(1),
        bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"),
    )?;
    assert_eq!(reg.len()?, 1);
    assert!(!reg.is_empty()?);
    Ok(())
}

#[test]
fn registry_table_names() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    reg.register(
        "a",
        RowId::new(1),
        bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid"),
    )?;
    reg.register(
        "a",
        RowId::new(2),
        bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid"),
    )?;
    reg.register(
        "b",
        RowId::new(3),
        bevy::prelude::Entity::from_raw_u32(3).expect("Entity index must be valid"),
    )?;

    let mut names = reg.table_names()?;
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
    Ok(())
}

#[test]
fn registry_configs_round_trip() -> BevyBridgeResult<()> {
    let reg = EntityMaterializationRegistry::new();
    reg.register_config(MaterializationConfig::new("provinces"))?;
    reg.register_config(MaterializationConfig::new("units").with_max_entities(10))?;

    let configs = reg.configs()?;
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].table, "provinces");
    assert_eq!(configs[1].max_entities, Some(10));
    Ok(())
}

// ------------------------------------------------------------------
// MaterializationFilter tests
// ------------------------------------------------------------------

#[test]
fn filter_all_rows_matches() {
    let filter = MaterializationFilter::AllRows;
    assert!(filter.matches_row_id(RowId::new(0)));
    assert!(filter.matches_row_id(RowId::new(999)));
}

#[test]
fn filter_row_id_range_matches() {
    let filter = MaterializationFilter::RowIdRange {
        start: RowId::new(5),
        end: RowId::new(10),
    };
    assert!(filter.matches_row_id(RowId::new(7)));
    assert!(!filter.matches_row_id(RowId::new(3)));
}

// ------------------------------------------------------------------
// InputCommandBuffer tests
// ------------------------------------------------------------------

#[test]
fn buffer_push_and_drain() -> BevyBridgeResult<()> {
    let buf = InputCommandBuffer::new();
    buf.set_tick(Tick(5))?;

    let cmd = Command::DeleteRow {
        table: "provinces".to_owned(),
        row: RowId::new(7),
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
fn buffer_push_ai_rejects() {
    let buf = InputCommandBuffer::new();
    let cmd = Command::Raw {
        domain: "war".to_owned(),
        payload: serde_json::Value::Null,
    };
    let result = buf.push_ai(cmd);
    assert!(matches!(
        result,
        Err(BevyBridgeError::NonPlayerCommandRejected)
    ));
}

#[test]
fn buffer_accepts_player_command_via_submit() -> BevyBridgeResult<()> {
    let buf = InputCommandBuffer::new();
    buf.set_tick(Tick(5))?;

    let cmd = Command::DeleteRow {
        table: "provinces".to_owned(),
        row: RowId::new(7),
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
    assert!(
        matches!(result, Err(BevyBridgeError::NonPlayerCommandRejected)),
        "expected NonPlayerCommandRejected, got {:?}",
        result
    );
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
        row: RowId::new(1),
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
fn buffer_player_id_tracks() -> BevyBridgeResult<()> {
    let buf = InputCommandBuffer::new().with_player_id(99)?;
    assert_eq!(buf.player_id()?, 99);

    buf.set_player_id(42)?;
    assert_eq!(buf.player_id()?, 42);
    Ok(())
}

// ------------------------------------------------------------------
// ViewModel tests
// ------------------------------------------------------------------

#[test]
fn view_model_initially_empty() -> BevyBridgeResult<()> {
    let vm = ViewModel::new();
    assert_eq!(vm.generation()?, 0);
    assert_eq!(vm.latest_tick()?, None);
    assert!(vm.snapshot()?.is_none());
    Ok(())
}

#[test]
fn view_model_refresh_updates_state() -> BevyBridgeResult<()> {
    let vm = ViewModel::new();
    let snapshot = Arc::new(scharnhorst_arrow_store::WorldSnapshot::new(Tick(7)));
    let view = WorldView::new(snapshot);

    vm.refresh(view, 3)?;

    assert_eq!(vm.generation()?, 3);
    assert_eq!(vm.latest_tick()?, Some(Tick(7)));
    assert!(vm.snapshot()?.is_some());
    Ok(())
}

#[test]
fn view_model_read_table_without_snapshot_fails() {
    let vm = ViewModel::new();
    let registry = SchemaRegistry::new();
    let qe = QueryEngine::new(registry);

    let result = vm.read_table_via_query_engine(&qe, "provinces");
    assert!(
        matches!(result, Err(BevyBridgeError::SnapshotNotAvailable(0))),
        "expected SnapshotNotAvailable, got {:?}",
        result
    );
}

// ------------------------------------------------------------------
// SyncState tests
// ------------------------------------------------------------------

#[test]
fn sync_state_register_and_count() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let entity = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    state.register_entity(entity, "provinces", 0, Tick(0))?;
    assert_eq!(state.entity_count()?, 1);
    Ok(())
}

#[test]
fn sync_state_mark_dirty_and_clear() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let entity = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    state.register_entity(entity, "provinces", 0, Tick(0))?;
    state.mark_dirty(entity)?;
    assert!(state.is_dirty(entity)?);
    assert_eq!(state.dirty_count()?, 1);

    state.clear_dirty()?;
    assert!(!state.is_dirty(entity)?);
    assert_eq!(state.dirty_count()?, 0);

    // Verify sync_all continues past individual failures and always clears dirty.
    let e1 = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    let e2 = bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid");
    state.register_entity(e1, "t1", 0, Tick(0))?;
    state.register_entity(e2, "t2", 0, Tick(0))?;
    state.mark_dirty(e1)?;
    state.mark_dirty(e2)?;

    let vm = ViewModel::new();
    let qe = QueryEngine::new(SchemaRegistry::new());
    let result = state.sync_all(&vm, &qe);
    assert!(
        matches!(result, Err(BevyBridgeError::SyncFailed(_))),
        "sync_all should aggregate errors as SyncFailed, got {:?}",
        result
    );
    // Both entities were attempted; dirty must still be cleared.
    assert_eq!(
        state.dirty_count()?,
        0,
        "dirty should be cleared even when sync_all fails"
    );

    Ok(())
}

#[test]
fn sync_state_mark_all_dirty() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let e1 = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    let e2 = bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid");
    state.register_entity(e1, "a", 0, Tick(0))?;
    state.register_entity(e2, "b", 0, Tick(0))?;
    state.mark_all_dirty()?;
    assert_eq!(state.dirty_count()?, 2);
    Ok(())
}

#[test]
fn sync_state_unregister_cleans_dirty() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let entity = bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    state.register_entity(entity, "provinces", 0, Tick(0))?;
    state.mark_dirty(entity)?;
    state.unregister_entity(entity)?;
    assert_eq!(state.entity_count()?, 0);
    assert_eq!(state.dirty_count()?, 0);
    Ok(())
}

/// May-fail: unregister_entity for an entity not in models must not panic.
///
/// Worst case: if unregister_entity panics or corrupts state when the
/// entity is absent, any cleanup path that double-unregisters triggers
/// undefined behaviour (e.g. stale dirty entries).
#[test]
fn unregister_entity_not_in_models_does_not_panic() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let entity = bevy::prelude::Entity::from_raw_u32(99).expect("Entity index must be valid");
    let result = state.unregister_entity(entity);
    assert!(
        result.is_ok(),
        "unregister_entity with unknown entity should not panic, got {:?}",
        result
    );
    assert_eq!(state.entity_count()?, 0);
    Ok(())
}

/// May-fail: sync_all with dirty entity not in models skips gracefully.
///
/// Worst case: a dangling dirty entry (from a race or stale handle)
/// causes sync_all to panic or produce incorrect sync counts, masking
/// real sync failures.
#[test]
fn sync_all_dirty_not_in_models_skips_gracefully() -> BevyBridgeResult<()> {
    let state = SyncState::new();
    let registered =
        bevy::prelude::Entity::from_raw_u32(1).expect("Entity index must be valid");
    let orphan =
        bevy::prelude::Entity::from_raw_u32(2).expect("Entity index must be valid");

    state.register_entity(registered, "provinces", 0, Tick(0))?;
    state.mark_dirty(registered)?;
    state.mark_dirty(orphan)?; // entity NOT in models

    let vm = ViewModel::new();
    let qe = QueryEngine::new(SchemaRegistry::new());

    // sync_all must not panic; the orphan is silently skipped via if-let.
    // The registered entity will fail (no snapshot), so SyncFailed is expected.
    let result = state.sync_all(&vm, &qe);
    assert!(
        matches!(result, Err(BevyBridgeError::SyncFailed(_))),
        "sync_all with dirty-not-in-models should aggregate errors, got {:?}",
        result
    );

    // Dirty must be cleared even for the orphaned entity.
    assert_eq!(
        state.dirty_count()?,
        0,
        "dirty must be cleared after sync_all"
    );
    Ok(())
}

// ------------------------------------------------------------------
// SnapshotRefreshHandler tests
// ------------------------------------------------------------------

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

#[test]
fn refresh_handler_signal_cycle() -> BevyBridgeResult<()> {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm.clone(), qe, store);
    let _ = handler.on_refresh_signal();
    assert!(handler.should_refresh()?);

    let view = WorldView::new(Arc::new(scharnhorst_arrow_store::WorldSnapshot::new(Tick(
        7,
    ))));
    handler.refresh_snapshot(view)?;

    assert!(!handler.should_refresh()?);
    assert_eq!(handler.current_generation()?, 1);
    assert_eq!(vm.generation()?, 1);
    Ok(())
}

#[test]
fn refresh_handler_callback_runs_without_panic() {
    let vm = Arc::new(ViewModel::new());
    let registry = SchemaRegistry::new();
    let qe = Arc::new(QueryEngine::new(registry));
    let store = Arc::new(ArrowStore::default());

    let handler = SnapshotRefreshHandler::new(vm, qe, store);
    let cb = handler.callback();
    let result = cb(1, 1);
    assert!(result.is_err());
}

#[test]
fn refresh_handler_config_builds() {
    let config = RefreshHandlerConfig::new("bevy_bridge");
    assert_eq!(config.consumer_name, "bevy_bridge");
}

// ------------------------------------------------------------------
// NullSyncField tests
// ------------------------------------------------------------------

#[test]
fn null_sync_field_returns_none() -> BevyBridgeResult<()> {
    let vm = ViewModel::new();
    let registry = SchemaRegistry::new();
    let qe = QueryEngine::new(registry);

    let result: Option<bevy::prelude::Transform> = NullSyncField.fetch(&vm, &qe, RowId::new(1))?;
    assert_eq!(result, None);
    Ok(())
}

// ------------------------------------------------------------------
// MaterializeRequest tests
// ------------------------------------------------------------------

#[test]
fn materialize_request_new() {
    let req = MaterializeRequest::new(RowId::new(5), "units");
    assert_eq!(req.row_id, RowId::new(5));
    assert_eq!(req.table_name, "units");
}

// ------------------------------------------------------------------
// MaterializationConfig tests
// ------------------------------------------------------------------

#[test]
fn materialization_config_builder_pattern() {
    let config = MaterializationConfig::new("provinces")
        .with_filter(MaterializationFilter::AllRows)
        .with_max_entities(100);

    assert_eq!(config.table, "provinces");
    assert!(config.filter.is_some());
    assert_eq!(config.max_entities, Some(100));
}

// ------------------------------------------------------------------
// Edge case tests
// ------------------------------------------------------------------

#[test]
fn input_buffer_drain_with_wrong_tick_rejected() -> BevyBridgeResult<()> {
    let buf = InputCommandBuffer::new();
    buf.set_tick(Tick(1))?;

    buf.push(Command::Raw {
        domain: "test".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    buf.prepare_for_tick(Tick(5))?;

    // drain_commands with tick different from prepared tick should fail
    let result = buf.drain_commands(Tick(6));
    assert!(
        matches!(result, Err(BevyBridgeError::TickAlignmentError { .. })),
        "expected TickAlignmentError, got {:?}",
        result
    );

    // Verify the expected/actual values in the error
    if let Err(BevyBridgeError::TickAlignmentError {
        expected, actual, ..
    }) = result
    {
        assert_eq!(expected, Tick(5));
        assert_eq!(actual, Tick(6));
    }

    Ok(())
}

#[test]
fn input_buffer_prepare_for_tick_resets_state() -> BevyBridgeResult<()> {
    let buf = InputCommandBuffer::new();
    buf.set_tick(Tick(1))?;

    // Push commands in tick 1
    buf.push(Command::Raw {
        domain: "a".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // Drain at tick 2
    buf.prepare_for_tick(Tick(2))?;
    let batch = buf.drain_commands(Tick(2))?;
    assert_eq!(batch.commands.len(), 1);

    // Push new commands
    buf.push(Command::Raw {
        domain: "b".to_owned(),
        payload: serde_json::Value::Null,
    })?;

    // prepare_for_tick with new tick should reset position,
    // old commands should still be pending
    buf.prepare_for_tick(Tick(3))?;
    let batch2 = buf.drain_commands(Tick(3))?;
    assert_eq!(batch2.commands.len(), 1);

    // After drain, buffer should be empty for the consumed tick
    assert!(!buf.is_prepared()?);
    Ok(())
}

#[test]
fn view_of_with_invalid_row_id() -> BevyBridgeResult<()> {
    // Set up a ViewModel with a real snapshot containing a table
    let store = Arc::new(ArrowStore::default());
    let init_store = InitStore::new(store.clone());

    let spec = TableSpec::new("test_table")
        .with_column(ColumnSpec::new("id", FieldSemantic::Id, "u64"))
        .expect("column")
        .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))
        .expect("column");

    init_store
        .create_table(&spec, scharnhorst_arrow_store::MutationMode::AppendOnly)
        .expect("create table");

    // Advance to simulation and generate a snapshot
    let commit_store = init_store.into_simulation().expect("into simulation");
    let snapshot = commit_store.generate_snapshot(Tick(1)).expect("snapshot");

    let vm = ViewModel::new();
    vm.refresh(WorldView::new(snapshot), 1)?;

    let registry = SchemaRegistry::new();
    let qe = QueryEngine::new(registry);

    // Querying a non-existent RowId using NullSyncField returns None
    // (the real SyncField implementations would query ArrowStore and also return None)
    let result: Option<bevy::prelude::Transform> =
        NullSyncField.fetch(&vm, &qe, RowId::new(99999))?;
    assert_eq!(result, None, "invalid RowId should return None");

    Ok(())
}
