use async_trait::async_trait;
use tokio::sync::mpsc;

// Rust trait does not support async fn methods across all compiler targets
// Trait representing any source of log lines
#[async_trait]
pub trait LogInput: Send + Sync {
    async fn run(self: Box<Self>, tx: mpsc::Sender<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

