//! scharnhorst_arrow_store: versioned Arrow storage, snapshots, indexing, and partitioning.

pub mod error;
pub mod index;
pub mod ipc_serialization;
pub mod partition;
pub mod snapshot;
pub mod store;
pub mod store_guard;
pub mod versioned_table;
pub mod world_view;

use arrow_array::RecordBatch;
use scharnhorst_core::{RowPositionMap, Tick};

pub use error::{ArrowStoreError, ArrowStoreResult};
pub use index::{ForeignKeyIndex, PrimaryKeyIndex, RowLocation};
pub use ipc_serialization::{deserialize_batches, serialize_batches, IpcBuffer};
pub use partition::{Partition, PartitionMap, PartitionSnapshot};
pub use snapshot::WorldSnapshot;
pub use store::{ArrowStore, JsonToArrayFn, NullArrayFn, TypeEntry, TypeRegistry};
pub use store_guard::{CommitStore, InitStore};
pub use versioned_table::{MutationMode, VersionedTable};
pub use world_view::{WorldView, WorldViewRow, WorldViewRowIter};

pub trait SnapshotIngestRollback: Send {
    fn rollback(self: Box<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

struct NoopSnapshotIngestRollback;

impl SnapshotIngestRollback for NoopSnapshotIngestRollback {
    fn rollback(self: Box<Self>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

pub trait SnapshotIngestor: Send + Sync {
    fn begin_ingest(
        &self,
    ) -> Result<Box<dyn SnapshotIngestRollback>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(NoopSnapshotIngestRollback))
    }

    fn ingest_snapshot(
        &self,
        tick: Tick,
        table_name: &str,
        batches: Vec<RecordBatch>,
        position_map: RowPositionMap,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn store_snapshot(
        &self,
        _snapshot: WorldSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
