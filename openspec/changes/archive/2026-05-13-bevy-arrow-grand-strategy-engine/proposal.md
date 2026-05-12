## Why

Existing grand strategy game engines often suffer from a tight coupling between simulation state and rendering, making them prone to non-determinism, difficult save/load migrations, and limited moddability. This project implements a high-performance, data-driven architecture that separates the columnar world state (Apache Arrow) from the interactive view (Bevy) and the logic rules (Rule IR).

## What Changes

- Introduction of a columnar data store based on Apache Arrow for world state.
- Implementation of a deterministic simulation scheduler with phase-based execution.
- A rule-based IR for triggers, effects, and modifiers to enable deep moddability.
- A command-diff journal system for perfect replayability, OOS detection, and incremental saves.
- A bridge layer to sync columnar state into Bevy ECS entities for rendering and interaction.
- A schema registry to handle table definitions, relations, and versioned migrations.

## Capabilities

### New Capabilities
- `schema-registry`: Management of table specs, field semantics, and versioning.
- `arrow-store`: Versioned columnar storage for state, relations, and event logs.
- `sim-scheduler`: Deterministic phase-based execution of domain systems.
- `journal-system`: Command and diff logging for replay and synchronization.
- `rule-ir`: Intermediate representation for moddable triggers and effects.
- `bevy-bridge`: Synchronization layer between Arrow state and Bevy entities.
- `content-loader`: Overlay-based loading of modded data and rule definitions.
- `query-engine`: High-performance data access and optional SQL analysis via DataFusion.
- `save-system`: Snapshot-based save/load with schema migration support.

### Modified Capabilities
- None

## Impact

- New project structure with multiple crates under the `scharnhorst_*` namespace. The following crate names are defined:
  - `scharnhorst_core`: ID types, fixed-point math, error handling
  - `scharnhorst_schema`: TableSpec, FieldSemantic, SchemaRegistry, RelationGraph
  - `scharnhorst_arrow_store`: Versioned columnar storage, WorldSnapshot
  - `scharnhorst_query`: Unified read-only query interface with DataFusion integration
  - `scharnhorst_journal`: Command and Diff types, single write entry point
  - `scharnhorst_scheduler`: Phase-based deterministic simulation loop
  - `scharnhorst_rules`: Rule IR, evaluator, modifier aggregation
  - `scharnhorst_content`: Mod overlay loader, content compilation, fingerprint generation
  - `scharnhorst_save`: Snapshot persistence, schema migration, checkpoint/replay
  - `scharnhorst_bevy`: Bevy bridge, entity materialization, command buffering
- Introduction of `arrow-rs` and `bevy` as core dependencies.
- Complete separation of simulation logic from rendering logic.
