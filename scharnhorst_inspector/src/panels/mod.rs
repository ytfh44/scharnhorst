//! Inspector panel implementations.

pub mod table_panel;
pub use table_panel::TablePanel;

pub mod diff_panel;
pub use diff_panel::DiffPanel;

pub mod relation_graph_panel;
pub use relation_graph_panel::RelationGraphPanel;

pub mod snapshot_browser_panel;
pub use snapshot_browser_panel::SnapshotBrowserPanel;
