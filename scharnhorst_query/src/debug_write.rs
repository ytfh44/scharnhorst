use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use scharnhorst_core::Tick;

/// An intercepted write operation observed by the debug journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DebugWriteOp {
    Append {
        table: String,
        tick: Tick,
        row_count: usize,
    },
    Patch {
        table: String,
        tick: Tick,
        row_indices: Vec<usize>,
    },
    Delete {
        table: String,
        tick: Tick,
        row_indices: Vec<usize>,
    },
    Rebuild {
        table: String,
        tick: Tick,
        row_count: usize,
    },
}

/// A ring-buffer journal that captures write operations in debug builds.
///
/// This is intended for developer tooling only and is compiled out (or made
/// inert) in release builds and in multiplayer sessions.
#[derive(Debug, Clone)]
pub struct DebugWriteJournal {
    inner: Arc<Mutex<JournalInner>>,
}

#[derive(Debug, Clone)]
struct JournalInner {
    capacity: usize,
    ops: VecDeque<DebugWriteOp>,
    enabled: bool,
}

impl DebugWriteJournal {
    pub fn new(capacity: usize) -> Self {
        let inner = JournalInner {
            capacity,
            ops: VecDeque::with_capacity(capacity),
            enabled: true,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.lock().map(|g| g.enabled).unwrap_or(false)
    }

    pub fn set_enabled(&self, enabled: bool) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.enabled = enabled;
        }
    }

    pub fn record(&self, op: DebugWriteOp) {
        if let Ok(mut guard) = self.inner.lock() {
            if !guard.enabled {
                return;
            }
            if guard.ops.len() >= guard.capacity {
                guard.ops.pop_front();
            }
            guard.ops.push_back(op);
        }
    }

    pub fn ops(&self) -> Vec<DebugWriteOp> {
        self.inner
            .lock()
            .map(|g| g.ops.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.ops.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.ops.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn latest(&self) -> Option<DebugWriteOp> {
        self.inner.lock().ok().and_then(|g| g.ops.back().cloned())
    }

    pub fn ops_for_table(&self, table: &str) -> Vec<DebugWriteOp> {
        self.inner
            .lock()
            .map(|g| {
                g.ops
                    .iter()
                    .filter(|op| op.table_name() == table)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Default for DebugWriteJournal {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl DebugWriteOp {
    pub fn table_name(&self) -> &str {
        match self {
            DebugWriteOp::Append { table, .. }
            | DebugWriteOp::Patch { table, .. }
            | DebugWriteOp::Delete { table, .. }
            | DebugWriteOp::Rebuild { table, .. } => table.as_str(),
        }
    }

    pub fn tick(&self) -> Tick {
        match self {
            DebugWriteOp::Append { tick, .. }
            | DebugWriteOp::Patch { tick, .. }
            | DebugWriteOp::Delete { tick, .. }
            | DebugWriteOp::Rebuild { tick, .. } => *tick,
        }
    }
}
