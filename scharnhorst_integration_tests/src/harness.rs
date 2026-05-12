//! Shared test harness for integration tests.
//!
//! Provides helpers to build a minimal world with actors, spatial nodes,
//! a journal, scheduler, query engine, and Bevy view model 鈥?all wired
//! together without `unwrap`/`expect`.

use std::sync::Arc;

use scharnhorst_arrow_store::{ArrowStore, MutationMode};
use scharnhorst_bevy::{InputCommandBuffer, ViewModel};
use scharnhorst_core::RowId;
use scharnhorst_journal::{
    Command, CommandEnvelope, Diff, DiffBatch, Journal,
};
use scharnhorst_query::engine::QueryEngine;
use scharnhorst_schema::{ColumnSpec, FieldSemantic, SchemaRegistry, TableSpec};
use scharnhorst_scheduler::{Scheduler, SimSystem, Phase};

/// Errors that can occur in the test harness.
#[derive(Debug)]
pub enum HarnessError {
    ArrowStore(scharnhorst_arrow_store::ArrowStoreError),
    Journal(scharnhorst_journal::JournalError),
    Scheduler(scharnhorst_scheduler::SchedulerError),
    Query(scharnhorst_query::QueryError),
    BevyBridge(scharnhorst_bevy::BevyBridgeError),
    Schema(scharnhorst_schema::SchemaError),
    Save(scharnhorst_save::SaveError),
    Content(scharnhorst_content::ContentError),
    Generic(String),
}

impl std::fmt::Display for HarnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HarnessError::ArrowStore(e) => write!(f, "arrow store: {e}"),
            HarnessError::Journal(e) => write!(f, "journal: {e}"),
            HarnessError::Scheduler(e) => write!(f, "scheduler: {e}"),
            HarnessError::Query(e) => write!(f, "query: {e}"),
            HarnessError::BevyBridge(e) => write!(f, "bevy bridge: {e}"),
            HarnessError::Schema(e) => write!(f, "schema: {e}"),
            HarnessError::Save(e) => write!(f, "save: {e}"),
            HarnessError::Content(e) => write!(f, "content: {e}"),
            HarnessError::Generic(s) => write!(f, "generic: {s}"),
        }
    }
}

impl std::error::Error for HarnessError {}

macro_rules! impl_from {
    ($variant:ident, $ty:ty) => {
        impl From<$ty> for HarnessError {
            fn from(value: $ty) -> Self {
                HarnessError::$variant(value)
            }
        }
    };
}

impl_from!(ArrowStore, scharnhorst_arrow_store::ArrowStoreError);
impl_from!(Journal, scharnhorst_journal::JournalError);
impl_from!(Scheduler, scharnhorst_scheduler::SchedulerError);
impl_from!(Query, scharnhorst_query::QueryError);
impl_from!(BevyBridge, scharnhorst_bevy::BevyBridgeError);
impl_from!(Schema, scharnhorst_schema::SchemaError);
impl_from!(Save, scharnhorst_save::SaveError);
impl_from!(Content, scharnhorst_content::ContentError);

pub type HarnessResult<T> = Result<T, HarnessError>;

/// A minimal world configuration used by integration tests.
pub struct TestWorld {
    pub arrow_store: ArrowStore,
    pub journal: Journal,
    pub scheduler: Scheduler,
    pub query_engine: QueryEngine,
    pub view_model: ViewModel,
    pub input_buffer: InputCommandBuffer,
}

impl TestWorld {
 /// Build a world with the standard MVP schema (actors, spatial_nodes, ownership).
    pub fn build_mvp() -> HarnessResult<Self> {
        let arrow_store = ArrowStore::new();
        let schema_registry = SchemaRegistry::new();

        let actors_spec = TableSpec::new("actors")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))?
            .with_column(ColumnSpec::new("name", FieldSemantic::Name, "utf8"))?
            .with_column(ColumnSpec::new("color", FieldSemantic::Tag, "utf8"))?;

        let nodes_spec = TableSpec::new("spatial_nodes")
            .with_column(ColumnSpec::new("id", FieldSemantic::Id, "i64"))?
            .with_column(ColumnSpec::new("x", FieldSemantic::Position2D, "f64"))?
            .with_column(ColumnSpec::new("y", FieldSemantic::Position2D, "f64"))?
            .with_column(ColumnSpec::new(
                "owner",
                FieldSemantic::ForeignKey { target_table: "actors".to_owned() },
                "i64",
            ))?;

        arrow_store.create_table(&actors_spec, MutationMode::AppendOnly)?;
        arrow_store.create_table(&nodes_spec, MutationMode::AppendOnly)?;

        let query_engine = QueryEngine::new(schema_registry);
        query_engine.register_table_schema(actors_spec)?;
        query_engine.register_table_schema(nodes_spec)?;

        let journal = Journal::new(arrow_store.clone());
        let scheduler = Scheduler::new(Journal::new(arrow_store.clone()), query_engine.clone());
        scheduler.initialize()?;

        let view_model = ViewModel::new();
        let input_buffer = InputCommandBuffer::new();

        Ok(Self {
            arrow_store,
            journal,
            scheduler,
            query_engine,
            view_model,
            input_buffer,
        })
    }

 /// Seed the world with 2 actors and 10 spatial nodes.
    pub fn seed_mvp_data(&mut self) -> HarnessResult<()> {
        let actor_ids: Vec<RowId> = (0..2).map(RowId::new).collect();
        let node_ids: Vec<RowId> = (0..10).map(RowId::new).collect();

        let actor_diffs: Vec<Diff> = actor_ids
            .iter()
            .enumerate()
            .map(|(idx, &row)| {
                let mut values = serde_json::Map::new();
                values.insert("id".to_owned(), serde_json::Value::Number(idx.into()));
                values.insert(
                    "name".to_owned(),
                    serde_json::Value::String(format!("actor_{}", idx)),
                );
                values.insert(
                    "color".to_owned(),
                    serde_json::Value::String(if idx == 0 { "red".to_owned() } else { "blue".to_owned() }),
                );
                Diff::Insert {
                    table: "actors".to_owned(),
                    row,
                    values,
                }
            })
            .collect();

        let node_diffs: Vec<Diff> = node_ids
            .iter()
            .enumerate()
            .map(|(idx, &row)| {
                let mut values = serde_json::Map::new();
                values.insert("id".to_owned(), serde_json::Value::Number((idx as u64 + 100).into()));
                values.insert("x".to_owned(), serde_json::Value::Number(((idx * 10) as u64).into()));
                values.insert("y".to_owned(), serde_json::Value::Number(((idx * 10) as u64).into()));
                values.insert(
                    "owner".to_owned(),
                    serde_json::Value::Number((idx % 2).into()),
                );
                Diff::Insert {
                    table: "spatial_nodes".to_owned(),
                    row,
                    values,
                }
            })
            .collect();

        let batch = DiffBatch {
            source: "harness_seed".to_owned(),
            diffs: actor_diffs.into_iter().chain(node_diffs).collect(),
        };

        self.journal.submit_batch(batch)?;
        self.journal.commit()?;
        Ok(())
    }

 /// Transfer ownership of a single node from one actor to another via command.
    pub fn transfer_node_owner(
        &mut self,
        node_id: RowId,
        from: RowId,
        to: RowId,
    ) -> HarnessResult<()> {
        let envelope = CommandEnvelope::new(
            self.scheduler.current_tick()?,
            "test_harness",
            Command::TransferControl {
                province_id: node_id,
                from_actor: from,
                to_actor: to,
            },
        );
        self.scheduler.enqueue_command(envelope)?;
        Ok(())
    }

 /// Register a [`SimSystem`] with the scheduler.
    pub fn register_system(&self, system: Arc<dyn SimSystem>) -> HarnessResult<()> {
        use scharnhorst_scheduler::BoxedSystem;
        self.scheduler.register_system(BoxedSystem::from(system))?;
        Ok(())
    }

 /// Advance the simulation by one tick.
    pub fn tick(&self) -> HarnessResult<scharnhorst_journal::CommitResult> {
        Ok(self.scheduler.tick()?)
    }
}

/// A no-op simulation system useful for tests that need a registered system.
pub struct NoOpSystem {
    id: String,
    phase: scharnhorst_scheduler::Phase,
}

impl NoOpSystem {
    pub fn new(id: impl Into<String>, phase: scharnhorst_scheduler::Phase) -> Self {
        Self {
            id: id.into(),
            phase,
        }
    }
}

impl SimSystem for NoOpSystem {
    fn id(&self) -> &str {
        &self.id
    }

    fn phase(&self) -> scharnhorst_scheduler::Phase {
        self.phase
    }

    fn read_tables(&self) -> Vec<String> {
        Vec::new()
    }

    fn write_tables(&self) -> Vec<String> {
        Vec::new()
    }

    fn execute(
        &self,
        _rng: &mut scharnhorst_scheduler::DeterministicRng,
        _query: &QueryEngine, _phase: Phase,
        _tick: u64,
    ) -> scharnhorst_scheduler::SchedulerResult<Vec<Diff>> {
        Ok(Vec::new())
    }
}
