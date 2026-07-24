//! A file sink that appends one JSON object per line (JSONL).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;

use agent_runtime_core::event::EventEnvelope;

use crate::{EventSink, ObsError};

/// Appends each event envelope as a JSON line to a file.
///
/// The full envelope is serialized (lossless), so the file can be replayed or
/// ingested by any JSONL-aware tool. The handle is opened once and reused; a
/// mutex keeps concurrent writes line-atomic.
#[derive(Debug)]
pub struct FileSink {
    path: PathBuf,
    file: Mutex<File>,
}

impl FileSink {
    /// Opens (creating if needed) `path` for append.
    pub fn jsonl(path: impl AsRef<Path>) -> Result<Self, ObsError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// The file this sink writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl EventSink for FileSink {
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError> {
        let json = serde_json::to_string(event)?;
        let mut file = self.file.lock().expect("poisoned");
        writeln!(file, "{json}")?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), ObsError> {
        self.file.lock().expect("poisoned").flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::sample_event;

    #[tokio::test]
    async fn appends_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let sink = FileSink::jsonl(&path).unwrap();
        sink.emit(&sample_event(0)).await.unwrap();
        sink.emit(&sample_event(1)).await.unwrap();
        sink.flush().await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["seq"], 0);
        assert_eq!(first["payload"]["event"], "text_delta");
    }
}
