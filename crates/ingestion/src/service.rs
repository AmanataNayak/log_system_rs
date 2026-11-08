use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use common::log_system::log_ingestion_service_server::LogIngestionService;
use common::log_system::{IngestResponse, LogEvent};
use tonic::{Request, Response, Status, Streaming};
use tracing::info;
use crate::buffer::{BatchConfig, LogBuffer};

#[derive(Debug)]
pub struct IngestionServer {
    tx: mpsc::Sender<LogEvent>,
}

impl IngestionServer {
    pub fn new(tx: mpsc::Sender<LogEvent>) -> Self {
        Self {
            tx
        }
    }
}

#[tonic::async_trait]
impl LogIngestionService for IngestionServer {
    async fn stream_logs(&self, request: Request<Streaming<LogEvent>>) -> Result<Response<IngestResponse>, Status> {
        let mut stream = request.into_inner();
        let mut count: u64 = 0;

        // Process incoming stream until client finishes or drops
        while let Some(log_event) = stream.message().await? {
            count += 1;
            if self.tx.send(log_event).await.is_err() {
                return Err(Status::internal("Batch flusher task shut down"));
            }
        }

        info!(target: "ingestion", "Stream finished. Total events: {}", count);

        let response = IngestResponse {
            success: true,
            count,
            error_message: None
        };

        Ok(Response::new(response))
    }
}


pub async fn run_batch_flusher(mut rx: mpsc::Receiver<LogEvent>, config: BatchConfig) {
    let interval_sec = config.max_flush_interval.as_secs();
    let mut buffer = LogBuffer::new(config);
    let mut ticker = interval(Duration::from_secs(interval_sec));

    loop {
        tokio::select! {
            // Trigger 1: New log event received over gRPC channel
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        if let Some(batch) = buffer.push(event) {
                            flush_batch(batch).await;
                            ticker.reset();
                        }
                    }
                    None => {
                        // gRPC stream closed: flush any remaining logs and exit
                        let final_batch = buffer.drain();
                        if !final_batch.is_empty() {
                            flush_batch(final_batch).await;
                        }
                        break;
                    }
                }
            }

            // Trigger 2: Time interval tick fired
            _ = ticker.tick() => {
                let batch = buffer.drain();
                if !batch.is_empty() {
                    info!("Flush triggered by timer timeout (count: {})", batch.len());
                    flush_batch(batch).await;
                }
            }
        }
    }

}


async fn flush_batch(batch: Vec<LogEvent>) {
    info!("Flushing micro-batch of {} events to storage layer", batch.len());
    // Next step: Convert batch to Apache Arrow RecordBatch and write Parquet!
}