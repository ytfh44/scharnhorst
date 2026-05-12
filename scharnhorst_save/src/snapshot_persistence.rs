use std::collections::HashMap;
use std::path::{Path, PathBuf};

use scharnhorst_content::SchemaManifest;
use scharnhorst_core::Tick;
use serde::{Deserialize, Serialize};

use crate::error::{SaveError, SaveResult};

/// Header embedded at the start of every snapshot file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotHeader {
 /// Schema manifest describing tables, relations, and mod fingerprints.
    pub schema_manifest: SchemaManifest,
 /// Generation number (tick) at which this snapshot was taken.
    pub generation: Tick,
 /// Deterministic hash of the world state at this generation.
    pub state_hash: u64,
}

impl SnapshotHeader {
    pub fn new(schema_manifest: SchemaManifest, generation: Tick, state_hash: u64) -> Self {
        Self {
            schema_manifest,
            generation,
            state_hash,
        }
    }
}

/// A persisted snapshot combining header metadata with table data.
pub struct PersistedSnapshot {
    pub header: SnapshotHeader,
 /// Table name -> serialized IPC bytes for the record batches.
    pub table_data: HashMap<String, Vec<u8>>,
}

impl PersistedSnapshot {
    pub fn new(header: SnapshotHeader) -> Self {
        Self {
            header,
            table_data: HashMap::new(),
        }
    }

    pub fn with_table_data(mut self, name: impl Into<String>, data: Vec<u8>) -> Self {
        self.table_data.insert(name.into(), data);
        self
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.table_data.keys().map(|s| s.as_str())
    }
}

/// Manages reading and writing snapshot files on disk.
pub struct SnapshotPersistence {
    base_dir: PathBuf,
}

impl SnapshotPersistence {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

 /// Compute the file path for a snapshot of the given generation.
    pub fn snapshot_path(&self, generation: Tick) -> PathBuf {
        self.base_dir
            .join(format!("snapshot_gen_{}.arrow", generation))
    }

 /// List all snapshot files in the base directory, sorted by generation ascending.
    pub fn list_snapshots(&self) -> SaveResult<Vec<(Tick, PathBuf)>> {
        let entries = std::fs::read_dir(&self.base_dir)
            .map_err(|e| SaveError::Io(format!("read_dir: {}", e)))?;

        let mut results: Vec<(Tick, PathBuf)> = entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                name_str
                    .strip_prefix("snapshot_gen_")
                    .and_then(|rest| rest.strip_suffix(".arrow"))
                    .and_then(|num| num.parse::<u64>().ok())
                    .map(|tick| (Tick(tick), entry.path()))
            })
            .collect();

        results.sort_by_key(|(tick, _)| *tick);
        Ok(results)
    }

 /// Return the latest generation snapshot path, if any.
    pub fn latest_snapshot(&self) -> SaveResult<Option<(Tick, PathBuf)>> {
        let mut snapshots = self.list_snapshots()?;
        Ok(snapshots.pop())
    }

 /// Write a persisted snapshot to disk.
    pub fn write_snapshot(&self, snapshot: &PersistedSnapshot) -> SaveResult<PathBuf> {
        let path = self.snapshot_path(snapshot.header.generation);
        if path.exists() {
            return Err(SaveError::SnapshotAlreadyExists(format!(
                "generation {}",
                snapshot.header.generation
            )));
        }

        let header_bytes = serde_json::to_vec(&snapshot.header).map_err(|e| {
            SaveError::IpcSerialization(format!("header json: {}", e))
        })?;

        let mut file = std::fs::File::create(&path)
            .map_err(|e| SaveError::Io(format!("create: {}", e)))?;

        use std::io::Write;
        let header_len = header_bytes.len() as u64;
        file.write_all(&header_len.to_le_bytes())
            .map_err(|e| SaveError::Io(format!("write header len: {}", e)))?;
        file.write_all(&header_bytes)
            .map_err(|e| SaveError::Io(format!("write header: {}", e)))?;

        let table_count = snapshot.table_data.len() as u64;
        file.write_all(&table_count.to_le_bytes())
            .map_err(|e| SaveError::Io(format!("write table count: {}", e)))?;

        for (name, data) in &snapshot.table_data {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u64;
            file.write_all(&name_len.to_le_bytes())
                .map_err(|e| SaveError::Io(format!("write name len: {}", e)))?;
            file.write_all(name_bytes)
                .map_err(|e| SaveError::Io(format!("write name: {}", e)))?;
            let data_len = data.len() as u64;
            file.write_all(&data_len.to_le_bytes())
                .map_err(|e| SaveError::Io(format!("write data len: {}", e)))?;
            file.write_all(data)
                .map_err(|e| SaveError::Io(format!("write data: {}", e)))?;
        }

        Ok(path)
    }

 /// Read a persisted snapshot from disk.
    pub fn read_snapshot(&self, generation: Tick) -> SaveResult<PersistedSnapshot> {
        let path = self.snapshot_path(generation);
        if !path.exists() {
            return Err(SaveError::SnapshotNotFound(format!(
                "generation {}",
                generation
            )));
        }

        let mut file = std::fs::File::open(&path)
            .map_err(|e| SaveError::Io(format!("open: {}", e)))?;

        use std::io::Read;
        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8)
            .map_err(|e| SaveError::Io(format!("read header len: {}", e)))?;
        let header_len = u64::from_le_bytes(buf8) as usize;

        let mut header_bytes = vec![0u8; header_len];
        file.read_exact(&mut header_bytes)
            .map_err(|e| SaveError::Io(format!("read header: {}", e)))?;
        let header: SnapshotHeader = serde_json::from_slice(&header_bytes).map_err(|e| {
            SaveError::IpcDeserialization(format!("header json: {}", e))
        })?;

        file.read_exact(&mut buf8)
            .map_err(|e| SaveError::Io(format!("read table count: {}", e)))?;
        let table_count = u64::from_le_bytes(buf8) as usize;

        let mut table_data = HashMap::with_capacity(table_count);
        for _ in 0..table_count {
            file.read_exact(&mut buf8)
                .map_err(|e| SaveError::Io(format!("read name len: {}", e)))?;
            let name_len = u64::from_le_bytes(buf8) as usize;
            let mut name_bytes = vec![0u8; name_len];
            file.read_exact(&mut name_bytes)
                .map_err(|e| SaveError::Io(format!("read name: {}", e)))?;
            let name = String::from_utf8(name_bytes).map_err(|e| {
                SaveError::CorruptedSnapshot(format!("invalid utf8 name: {}", e))
            })?;

            file.read_exact(&mut buf8)
                .map_err(|e| SaveError::Io(format!("read data len: {}", e)))?;
            let data_len = u64::from_le_bytes(buf8) as usize;
            let mut data = vec![0u8; data_len];
            file.read_exact(&mut data)
                .map_err(|e| SaveError::Io(format!("read data: {}", e)))?;

            table_data.insert(name, data);
        }

        Ok(PersistedSnapshot {
            header,
            table_data,
        })
    }

 /// Delete a snapshot file for the given generation.
    pub fn delete_snapshot(&self, generation: Tick) -> SaveResult<()> {
        let path = self.snapshot_path(generation);
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| SaveError::Io(format!("delete snapshot: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("sch_save_test_{}", std::process::id()))
    }

    #[test]
    fn snapshot_roundtrip() -> SaveResult<()> {
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).map_err(|e| SaveError::Io(e.to_string()))?;

        let persistence = SnapshotPersistence::new(&dir);
        let manifest = SchemaManifest::new("0.1.0");
        let header = SnapshotHeader::new(manifest, Tick(42), 0xdeadbeef);
        let snapshot = PersistedSnapshot::new(header)
            .with_table_data("actors", vec![1, 2, 3])
            .with_table_data("provinces", vec![4, 5, 6]);

        let written = persistence.write_snapshot(&snapshot)?;
        assert!(written.exists());

        let loaded = persistence.read_snapshot(Tick(42))?;
assert_eq!(loaded.header.generation, Tick(42));
        assert_eq!(loaded.header.state_hash, 0xdeadbeef);
        assert_eq!(loaded.table_data.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }
}
