//! scharnhorst_content: content loader, overlay resolver, name resolution,
//! compilation pipeline, fingerprint generation, schema manifest, and load lifecycle.

pub mod compilation;
pub mod error;
pub mod fingerprint;
pub mod lifecycle;
pub mod manifest;
pub mod name_resolution;
pub mod overlay;

pub use compilation::{CompilationInput, CompilationOutput, ContentCompiler, RawTableDef};
pub use error::{ContentError, ContentResult};
pub use fingerprint::{FingerprintComparison, FingerprintRegistry, ModFingerprintExt};
pub use lifecycle::{LifecycleCoordinator, LoadLifecycle, LoadPhase};
pub use manifest::{
    manifest_engine_version, manifest_from_bytes, manifest_from_registry, manifest_from_toml,
    manifest_to_bytes, manifest_to_toml, migrated_from_manifest, migrated_table_by_name,
    migrated_target_version,
};
pub use name_resolution::{NameResolver, NamespaceResolver};
pub use overlay::{MergeStrategy, OverlayEntry, OverlayLayer, OverlayResolver};

// Re-export canonical types from scharnhorst_schema
pub use scharnhorst_schema::manifest::ModFingerprint;
pub use scharnhorst_schema::manifest::{MigratedSchemaManifest, SchemaManifest};
