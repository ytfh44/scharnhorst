## MODIFIED Requirements

### Requirement: Read-Only Snapshot Access (query-engine, sim-scheduler)
The `bevy-bridge` SHALL obtain committed world data via `query-engine` to drive entity materialization and view model synchronization. The bridge:

- MUST NOT hold mutable references to Arrow tables.
- MUST NOT bypass `journal-system` to write diffs.
- MUST NOT reference raw Arrow `RecordBatch` or table internals directly.
- MUST NOT query the schema-registry `RelationGraph` directly.
- SHALL refresh read-only committed data after receiving `REFRESH_SIGNAL`.
- SHALL treat Bevy ECS components, view models, hover state, animation state, and frame-local caches as Derived or Ephemeral state, not Authority state.

**Component lifetime constraint:**
All Bevy Component types defined in `scharnhorst_bevy` SHALL satisfy `Component: 'static`. Fields containing string data SHALL use owned `String` rather than borrowed `&'static str`.

**REFRESH_SIGNAL protocol:**

```
T+0 end of PostTick:
1. sim-scheduler broadcasts REFRESH_SIGNAL.
2. bevy-bridge receives signal.

T+1 before next frame sync:
3. bridge discards old committed data handles.
4. bridge obtains fresh read-only data via query-engine.
5. entity synchronization proceeds with generation N+1 data.
```

#### Scenario: Bridge reads after commit
- **WHEN** `sim-scheduler` commits a tick and signals consumers
- **THEN** the bridge discards old committed data handles
- **AND** synchronizes entities from fresh query-engine data
- **AND** cannot mutate Authority state through the read handle

#### Scenario: Bridge view state excluded from saves
- **WHEN** a province entity has hover and animation components
- **THEN** those components are treated as Ephemeral Bevy state
- **AND** they are not persisted as Authority state

### Requirement: Command Buffering
The bridge SHALL buffer player interactions into a command queue. These commands are consumed by the `sim-scheduler` at the start of each tick and submitted to `journal-system` as command objects.

The bridge MUST NOT translate player interactions into direct `ArrowStore` mutations. Authority state changes occur only if submitted commands eventually produce journal-committed diffs.

#### Scenario: Clicking a button
- **WHEN** a player clicks `Declare War` in the UI
- **THEN** the bridge pushes a command into the `InputCommandBuffer`
- **AND** no Authority state changes until the journal commit path applies resulting diffs

## ADDED Requirements

### Requirement: Bevy State Tier Ownership
The Bevy bridge SHALL document or register state-tier ownership for its runtime state:

- Player command buffer: Ephemeral until consumed into journal submission.
- Entity materialization registry: Derived or Ephemeral view mapping.
- View models and sync fields: Derived views of Authority state.
- Hover, selection, animation, and camera state: Ephemeral.

#### Scenario: Materialization registry rebuild
- **WHEN** a committed snapshot changes visible province ownership
- **THEN** Bevy view synchronization updates Derived view state from query-engine data
- **AND** the materialization registry is not treated as Authority state

### Requirement: No Simulation-Origin Commands Through Bridge
The Bevy bridge SHALL only buffer player-originated commands. AI and internal simulation systems submit directly to journal-system through scheduler-provided capabilities.

#### Scenario: AI declares war
- **WHEN** an AI system decides to declare war
- **THEN** it submits through journal-system or scheduler-provided journal capability
- **AND** it does not use the Bevy input buffer

### Requirement: Lock Poisoning Propagation
All Mutex lock acquisitions in `scharnhorst_bevy` production code SHALL propagate lock poisoning as `BevyBridgeError::LockPoisoned` rather than silently swallowing the poison via `if let Ok(...)` or `.ok()`. This includes `with_player_id` on `InputCommandBuffer` and all `SyncState` lock methods. Lock poisoning indicates an unrecoverable invariants violation in another subsystem and SHALL be surfaced to the caller.

#### Scenario: Poisoned lock surfaced
- **WHEN** any `scharnhorst_bevy` Mutex is poisoned by a panic in another subsystem
- **THEN** subsequent lock acquisitions return `BevyBridgeError::LockPoisoned`
- **AND** the error propagates to the application-level recovery or panic handler
