use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;

use crate::error::{ArrowStoreError, ArrowStoreResult};

/// Serialize a slice of [`RecordBatch`]es into Arrow IPC stream bytes.
pub fn serialize_batches(batches: &[RecordBatch]) -> ArrowStoreResult<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| ArrowStoreError::Io(format!("stream writer: {}", e)))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| ArrowStoreError::Io(format!("write batch: {}", e)))?;
        }
        writer
            .finish()
            .map_err(|e| ArrowStoreError::Io(format!("finish: {}", e)))?;
    }
    Ok(buf)
}

/// Deserialize Arrow IPC stream bytes into a vector of [`RecordBatch`]es.
pub fn deserialize_batches(data: &[u8]) -> ArrowStoreResult<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| ArrowStoreError::Io(format!("stream reader: {}", e)))?;
    reader
        .map(|result| result.map_err(|e| ArrowStoreError::Io(format!("read batch: {}", e))))
        .collect::<ArrowStoreResult<Vec<_>>>()
}

/// A sink that accumulates Arrow IPC bytes for incremental serialization.
#[derive(Debug, Clone, Default)]
pub struct IpcBuffer {
    chunks: Vec<Vec<u8>>,
}

impl IpcBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a pre-serialized IPC chunk.
    pub fn push(&mut self, chunk: Vec<u8>) {
        self.chunks.push(chunk);
    }

    /// Serialize batches and append the resulting bytes.
    pub fn push_batches(&mut self, batches: &[RecordBatch]) -> ArrowStoreResult<()> {
        let bytes = serialize_batches(batches)?;
        self.push(bytes);
        Ok(())
    }

    /// Return the total byte length of all chunks.
    pub fn total_len(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    /// Return the number of stored chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Iterate over all chunks.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks.iter().map(|c| c.as_slice())
    }

    /// Clear all accumulated chunks.
    pub fn clear(&mut self) {
        self.chunks.clear();
    }

    /// Concatenate all chunks into a single byte vector.
    pub fn into_bytes(self) -> Vec<u8> {
        let total = self.total_len();
        let mut out = Vec::with_capacity(total);
        for chunk in self.chunks {
            out.extend_from_slice(&chunk);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ArrowStoreResult;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, StringArray};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        RecordBatch::try_new(schema, vec![id, name]).unwrap_or_else(|_| panic!("test batch"))
    }

    #[test]
    fn roundtrip_batches() -> ArrowStoreResult<()> {
        let batch = sample_batch();
        let bytes = serialize_batches(std::slice::from_ref(&batch))?;
        let decoded = deserialize_batches(&bytes)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
        Ok(())
    }

    #[test]
    fn empty_roundtrip() -> ArrowStoreResult<()> {
        let bytes = serialize_batches(&[])?;
        assert!(bytes.is_empty());
        let decoded = deserialize_batches(&bytes)?;
        assert!(decoded.is_empty());
        Ok(())
    }

    #[test]
    fn ipc_buffer_accumulates() -> ArrowStoreResult<()> {
        let batch = sample_batch();
        let mut buf = IpcBuffer::new();
        buf.push_batches(&[batch])?;
        assert_eq!(buf.chunk_count(), 1);
        assert!(buf.total_len() > 0);
        Ok(())
    }
}
