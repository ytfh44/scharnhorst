//! scharnhorst_schema: table definitions, field semantics, schema registry, and relations.
//!
//! # Six-Phase Load Lifecycle
//!
//! This crate participates in the six-phase load lifecycle as defined in the
//! save-system specification :
//!
//! | Phase | Action | Schema-Registry Role |
//! |-------|--------|---------------------|
//! | **Phase 1** | Snapshot Deserialize | Parse `SchemaManifest` from save header |
//! | **Phase 2** | Mod Coordination | Store `ModFingerprint` list for comparison |
//! | **Phase 3** | Schema Migration | Apply migrations via `MigrationRegistry` |
//! | **Phase 4** | Content Compilation | Receive `MigratedSchemaManifest`; register `TableSpec`s; build `RelationGraph` |
//! | **Phase 5** | Schema Freeze | Set `is_frozen = true` |
//! | **Phase 6** | Simulation Start | No role (frozen) |

pub mod error;
pub mod field_semantic;
pub mod manifest;
pub mod migration;
pub mod relation;
pub mod registry;
pub mod table_spec;

pub use error::{SchemaError, SchemaResult};
pub use field_semantic::FieldSemantic;
pub use manifest::{MigratedSchemaManifest, ModFingerprint, SchemaManifest};
pub use migration::{Migration, MigrationFn, MigrationRegistry, MigratedSchemaManifestExt};
pub use relation::{RelationEdge, RelationGraph, RelationKind};
pub use registry::SchemaRegistry;
pub use table_spec::{ColumnSpec, TableSpec};
