**Crate**: `scharnhorst_journal`

## ADDED Requirements

### Requirement: Command Logging
The system SHALL capture all external and internal intents as `Command` objects before they affect the world state.

#### Scenario: Player input
- **WHEN** a player transfers control of a province
- **THEN** a `TransferControl` command is appended to the command journal with a timestamp and player ID.

### Requirement: Diff Generation
The simulation systems MUST produce `Diff` objects rather than mutating tables directly.

#### Scenario: Treasury update
- **WHEN** a system calculates tax income for an actor
- **THEN** it emits a `Diff::Update` for the "treasury" column of that actor's row.

### Requirement: Atomic Commit
The system SHALL apply all diffs for a tick atomically, producing a new world generation and a single state hash.

#### Scenario: State commit
- **WHEN** the simulation reaches the end of the tick
- **THEN** all pending diffs are merged, indices are refreshed, and a new `WorldSnapshot` is published.

## MODIFIED Requirements

### (Compatibility) Requirement: Single Write Entry Point (↔ arrow-store, ↔ bevy-bridge, ↔ rule-ir, ↔ sim-scheduler)
The journal system is the **sole write path** for all world state mutations. No component may write directly to Arrow tables. The following write sources are defined:

| Source | Write Path |
|--------|-----------|
| Player commands (via `bevy-bridge`) | `bevy-bridge.InputCommandBuffer` → `sim-scheduler` (tick start) → `journal.submit()` |
| AI / internal systems | Direct call to `journal.submit()` |
| Rule-IR effects | Evaluator → `journal.submit()` |
| Debug/developer writes (debug build only) | `DebugWriteJournal` → `journal.submit()` |

**Invariant**: At any point, `ArrowStore.current_snapshot` reflects the result of all committed diffs up to generation N, and no uncommitted diffs have been applied.

#### DebugWriteJournal Implementation (DC-4)

The `DebugWriteJournal` is a **debug-only** component that intercepts SQL `UPDATE`/`INSERT`/`DELETE` statements from developer tooling and translates them into standard `Diff` objects:

```
Debug Build Path:
  Developer SQL (e.g., "UPDATE actor_state SET treasury = 1000 WHERE actor_id = 1")
    ↓
  query-engine SQL parser
    ↓
  DebugWriteJournal intercepts (production builds: REJECT)
    ↓
  Translate to Diff::Update { table: "actor_state", row: 1, column: "treasury", value: 1000 }
    ↓
  Submit to journal-system (same as normal Diff)
    ↓
  Committed at tick boundary (preserves determinism)
```

**Constraints**:
- **Debug builds only**: Automatically disabled in production/multiplayer builds
- **No direct writes**: Still routes through journal, preserving single-write invariant
- **Tick-aligned**: Changes committed at next tick boundary, not immediately
- **Logged**: All debug writes are logged for debugging reproducibility

**Interface**:
```rust
#[cfg(debug_assertions)]
pub struct DebugWriteJournal {
    enabled: bool, // false in multiplayer
    pending_diffs: Vec<Diff>,
}

#[cfg(debug_assertions)]
impl DebugWriteJournal {
    pub fn execute_sql(&mut self, sql: &str) -> Result<()> {
        // Parse SQL, translate to Diff, submit to journal
    }
}
```

#### Invariant:
At any point, `ArrowStore.current_snapshot` reflects the result of all committed diffs up to generation N, and no uncommitted diffs have been applied.

### (Compatibility) Requirement: Tick-Aligned Commit Cycle (↔ sim-scheduler, ↔ bevy-bridge, ↔ query-engine)
Commands consumed from the bridge buffer at tick start are submitted to the journal **before** simulation systems execute. All diffs produced during the tick are batched and committed **atomically** at tick end.

```
Tick lifecycle:
T+0 (start):
  1. scheduler pulls bridge commands → journal.submit(commands)
  2. scheduler runs [PreTick, Economy, Diplomacy, ...PostTick] systems
  3. systems emit Diffs → journal accumulates

T+1 (end of PostTick):
  4. scheduler triggers journal.commit()
  5. diffs applied atomically to ArrowStore → new snapshot (generation N+1) published
  6. scheduler broadcasts REFRESH_SIGNAL to all consumers (sim-scheduler systems, rule-ir evaluator, bevy-bridge)
  7. consumers acknowledge signal (discard old snapshot references, clear caches)
  8. SaveJournal appended
```

**See**: `sim-scheduler` for the complete REFRESH_SIGNAL protocol (DC-10).

### (Compatibility) Requirement: SaveJournal for Incremental Persistence (↔ save-system)
Each tick's accumulated diffs SHALL be serialized to the `SaveJournal` (append-only IPC file) immediately after commit. The SaveJournal is a flat sequence of `(tick_number, Vec<Diff>)` entries.

#### Scenario: Incremental save
- **WHEN** the player quick-saves
- **THEN** the system writes the SaveJournal since the last checkpoint. No full snapshot is generated.
- **WHEN** the player does a full save
- **THEN** the system writes a full `WorldSnapshot` and truncates the SaveJournal.
