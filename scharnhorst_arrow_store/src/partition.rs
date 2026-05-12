use std::collections::HashMap;

use arrow_array::RecordBatch;
use scharnhorst_core::Tick;

use crate::error::{ArrowStoreError, ArrowStoreResult};

/// A partition holds a subset of a table's batches scoped to a region.
#[derive(Debug, Clone, Default)]
pub struct Partition {
    pub region_id: String,
    pub batches: Vec<RecordBatch>,
}

impl Partition {
    pub fn new(region_id: impl Into<String>) -> Self {
        Self {
            region_id: region_id.into(),
            batches: Vec::new(),
        }
    }

    pub fn region_id(&self) -> &str {
        &self.region_id
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn add_batch(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

/// Maps region identifiers to their associated partitions.
#[derive(Debug, Clone, Default)]
pub struct PartitionMap {
    partitions: HashMap<String, Partition>,
}

impl PartitionMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, region_id: &str) -> ArrowStoreResult<&Partition> {
        self.partitions
            .get(region_id)
            .ok_or_else(|| ArrowStoreError::PartitionNotFound(region_id.to_owned()))
    }

    pub fn get_mut(&mut self, region_id: &str) -> ArrowStoreResult<&mut Partition> {
        self.partitions
            .get_mut(region_id)
            .ok_or_else(|| ArrowStoreError::PartitionNotFound(region_id.to_owned()))
    }

    pub fn get_or_create(&mut self, region_id: &str) -> &mut Partition {
        self.partitions
            .entry(region_id.to_owned())
            .or_insert_with(|| Partition::new(region_id))
    }

    pub fn insert(&mut self, region_id: impl Into<String>, partition: Partition) {
        self.partitions.insert(region_id.into(), partition);
    }

    pub fn remove(&mut self, region_id: &str) -> Option<Partition> {
        self.partitions.remove(region_id)
    }

    pub fn contains(&self, region_id: &str) -> bool {
        self.partitions.contains_key(region_id)
    }

    pub fn region_ids(&self) -> impl Iterator<Item = &str> {
        self.partitions.keys().map(|s| s.as_str())
    }

    pub fn partitions(&self) -> impl Iterator<Item = &Partition> {
        self.partitions.values()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }
}

/// A snapshot view of a single partition at a specific tick.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    pub region_id: String,
    pub tick: Tick,
    pub batches: Vec<RecordBatch>,
}

impl PartitionSnapshot {
    pub fn new(region_id: impl Into<String>, tick: Tick, batches: Vec<RecordBatch>) -> Self {
        Self {
            region_id: region_id.into(),
            tick,
            batches,
        }
    }

    pub fn region_id(&self) -> &str {
        &self.region_id
    }

    pub fn tick(&self) -> Tick {
        self.tick
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}
