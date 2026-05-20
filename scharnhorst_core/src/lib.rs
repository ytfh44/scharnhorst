//! scharnhorst_core: foundational types, math, and error handling.

pub mod capability;
pub mod diff;
pub mod error;
pub mod fixed_point;
pub mod id;
pub mod row_position;
pub mod state_tier;

pub use capability::{
    CommitToken, InitToken, JournalSubmitToken, Lifecycle, LifecycleGuard, SimReadToken,
};
pub use diff::{Diff, DiffBatch};
pub use error::{CoreError, CoreResult};
pub use fixed_point::FixedPoint;
pub use id::{RowId, TableId, Tick};
pub use row_position::{RowLookup, RowPositionMap};
pub use state_tier::{DerivedState, StateTier, TierMetadata, TierRegistry};
