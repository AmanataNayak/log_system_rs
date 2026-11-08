use async_trait::async_trait;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

// Rust trait does not support async fn methods across all compiler targets
// Trait representing any source of log lines
#[async_trait]
pub trait LogInput: Send + Sync {
    async fn run(self, tx: mpsc::Sender<String>) -> io::Result<()>;
}


pub struct StdinInput;

#[async_trait]
impl LogInput for StdinInput {
    async fn run(self, tx: mpsc::Sender<String>) -> io::Result<()>{
        let stdin = io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            if tx.send(line).await.is_err(){
                break;
            }
        }
        Ok(())
    }
}

