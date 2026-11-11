use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCommitEvent {
    pub catalog_name: String,
    pub namespace: String,
    pub table_name: String,
    pub snapshot_id: i64,
    pub timestamp_ms: i64
}
