use async_trait::async_trait;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use crate::input::LogInput;

pub struct StdinInput;

#[async_trait]
impl LogInput for StdinInput {
    async fn run(self: Box<Self>, tx: mpsc::Sender<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>{
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

