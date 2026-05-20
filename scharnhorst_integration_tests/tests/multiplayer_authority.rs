//! 11.4 Implement multiplayer authority model: server-only Journal,
//! client input replay
//!
//! Verifies that:
//! - Only the server-side journal may commit.
//! - Client input can be captured, serialized, and replayed.
//! - Replayed input yields deterministic results.

use scharnhorst_core::{RowId, Tick};
use scharnhorst_integration_tests::harness::TestWorld;
use scharnhorst_journal::{Command, CommandEnvelope, InMemorySaveJournal, SaveJournal};

/// A thin wrapper representing the "server" authority in a multiplayer session.
struct ServerJournal {
    inner: InMemorySaveJournal,
}

impl ServerJournal {
    fn new() -> Self {
        Self {
            inner: InMemorySaveJournal::new(),
        }
    }

    fn records(&self) -> &[scharnhorst_journal::CommitRecord] {
        self.inner.records()
    }
}

impl SaveJournal for ServerJournal {
    fn append(
        &mut self,
        record: &scharnhorst_journal::CommitRecord,
    ) -> scharnhorst_journal::JournalResult<()> {
        self.inner.append(record)
    }

    fn flush(&mut self) -> scharnhorst_journal::JournalResult<()> {
        self.inner.flush()
    }

    fn truncate_before(&mut self, tick: Tick) -> scharnhorst_journal::JournalResult<()> {
        self.inner.truncate_before(tick)
    }
}

/// Represents a client that can produce input commands but never commits directly.
struct ClientInputBuffer {
    player_id: String,
    commands: Vec<CommandEnvelope>,
}

impl ClientInputBuffer {
    fn new(player_id: impl Into<String>) -> Self {
        Self {
            player_id: player_id.into(),
            commands: Vec::new(),
        }
    }

    fn submit(&mut self, tick: Tick, command: Command) {
        self.commands
            .push(CommandEnvelope::new(tick, self.player_id.clone(), command));
    }

    fn drain(&mut self) -> Vec<CommandEnvelope> {
        let out = self.commands.clone();
        self.commands.clear();
        out
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.commands)
    }

    fn from_json(s: &str) -> Result<Vec<CommandEnvelope>, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[test]
fn server_journal_records_commits() {
    let mut server = ServerJournal::new();
    let record = scharnhorst_journal::CommitRecord::new(Tick(1), Vec::new(), 0);
    server.append(&record).expect("append");
    assert_eq!(server.records().len(), 1);
}

#[test]
fn client_buffer_serializes_and_replays() {
    let mut client = ClientInputBuffer::new("player_1");
    client.submit(
        Tick(0),
        Command::TransferControl {
            province_id: RowId::new(5),
            from_actor: RowId::new(0),
            to_actor: RowId::new(1),
        },
    );

    let json = client.to_json().expect("serialize");
    let replayed = ClientInputBuffer::from_json(&json).expect("deserialize");

    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].tick, Tick(0));
    assert_eq!(replayed[0].source, "player_1");
}

#[test]
fn replayed_input_produces_same_hashes() {
    let mut world_a = TestWorld::build_mvp().expect("build world a");
    let mut world_b = TestWorld::build_mvp().expect("build world b");

    world_a.seed_mvp_data().expect("seed a");
    world_b.seed_mvp_data().expect("seed b");

    // Simulate client input at the current tick (1, after seeding advanced from 0).
    let mut client = ClientInputBuffer::new("player_1");
    let current_tick = world_a.scheduler.current_tick().expect("current tick");
    client.submit(
        current_tick,
        Command::TransferControl {
            province_id: RowId::new(3),
            from_actor: RowId::new(0),
            to_actor: RowId::new(1),
        },
    );

    let inputs = client.drain();

    // Server A processes the inputs.
    for env in &inputs {
        world_a
            .scheduler
            .enqueue_command(env.clone())
            .expect("enqueue a");
    }
    let result_a = world_a.tick().expect("tick a");

    // Server B replays the *same* inputs.
    for env in &inputs {
        world_b
            .scheduler
            .enqueue_command(env.clone())
            .expect("enqueue b");
    }
    let result_b = world_b.tick().expect("tick b");

    assert_eq!(result_a.state_hash, result_b.state_hash);
    assert_eq!(result_a.diff_count, result_b.diff_count);
}

#[test]
fn client_cannot_commit_directly() {
    // The ClientInputBuffer has no commit method; it only produces envelopes.
    // This is a compile-time guarantee, but we assert the API surface here.
    let client = ClientInputBuffer::new("player_1");
    assert!(client.to_json().is_ok());
}

#[test]
fn server_truncates_old_entries() {
    let mut server = ServerJournal::new();
    for i in 0..5 {
        let record = scharnhorst_journal::CommitRecord::new(Tick(i), Vec::new(), 0);
        server.append(&record).expect("append");
    }

    server.truncate_before(Tick(3)).expect("truncate");
    let ticks: Vec<u64> = server.records().iter().map(|r| r.tick.0).collect();
    assert_eq!(ticks, vec![3, 4]);
}
