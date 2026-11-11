use std::collections::HashMap;
use std::fs::read;
use std::io::{Seek, SeekFrom};
use tokio::fs::{File};
use tokio::io::{BufReader, AsyncBufReadExt};
use std::path::PathBuf;
use async_trait::async_trait;
use glob::{Paths, PatternError};
use tokio::io;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{info, error, warn};
use crate::input::LogInput;

pub struct FileInput {
    pub glob_pattern: String,
    pub poll_interval: Duration
}


impl FileInput {
    pub fn new(glob_pattern: String, poll_interval: u64) -> Self {
        Self {
            glob_pattern,
            poll_interval: Duration::from_millis(poll_interval)
        }
    }
}

#[async_trait]
impl LogInput for FileInput {
    async fn run(self: Box<Self>, tx: mpsc::Sender<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>{
        info!(target: "collector", "Tailing files with patterns: {}", self.glob_pattern);

        // We track file offsets (how many bytes we have read) so we don't re-read old logs.
        let mut file_position: HashMap<PathBuf, u64> = HashMap::new();

        loop {
            // Expands wildcards paths like "data/logs/*.log" into a concrete list of matching file paths.
            let paths: Vec<PathBuf> = match glob::glob(&self.glob_pattern) {
                Ok(entries) => entries.filter_map(Result::ok).collect(),
                Err(e) => {
                    error!(target: "collector", "Invalid glob pattern: {}", e);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            for path in paths {
                let offset = *file_position.get(&path).unwrap_or(&0);

                if let Ok(file) = File::open(&path).await {
                    let metadata = match file.metadata().await {
                        Ok(meta) => meta,
                        Err(_) => continue,
                    };

                    let file_size = metadata.len();
                    let current_offset = if file_size < offset { 0 } else { offset };

                    if file_size > current_offset {
                        // Convert Tokio File to std File temporarily to execute seek operation
                        let mut std_file = file.into_std().await;
                        use std::io::Seek;

                        if std_file.seek(SeekFrom::Start(current_offset)).is_ok() {
                            let file = File::from_std(std_file);
                            let mut reader = BufReader::new(file);
                            let mut line = String::new();

                            while let Ok(bytes_read) = reader.read_line(&mut line).await {
                                if bytes_read == 0 {
                                    break;
                                }

                                let trimmed = line.trim_end().to_string();

                                if !trimmed.is_empty() {
                                    tx.send(trimmed).await?;
                                }
                                line.clear();
                            }
                            if let Ok(new_offset) = reader.into_inner().into_std().await.seek(SeekFrom::Current(0)) {
                                file_position.insert(path, new_offset);
                            }
                        }
                    }
                }
            }

            sleep(self.poll_interval).await;
        }
    }
}
