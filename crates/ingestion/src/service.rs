use std::sync::Arc;
use std::time::Duration;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use tokio::sync::mpsc;
use tokio::time::interval;
use common::log_system::log_ingestion_service_server::LogIngestionService;
use common::log_system::{IngestResponse, LogEvent};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, error};
use crate::arrow::events_record_batch;
use crate::buffer::{BatchConfig, LogBuffer};
use crate::iceberg_catalog::IcebergCatalogManager;
use crate::parquet_writer::ParquetWriter;
use crate::sql_catalog::PostgresIcebergCatalogManger;

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


pub async fn run_batch_flusher(mut rx: mpsc::Receiver<LogEvent>, config: BatchConfig, output_dir: String, catalog: Arc<PostgresIcebergCatalogManger>) {
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
                            flush_batch(batch, &output_dir, &catalog).await;
                            ticker.reset();
                        }
                    }
                    None => {
                        // gRPC stream closed: flush any remaining logs and exit
                        let final_batch = buffer.drain();
                        if !final_batch.is_empty() {
                            flush_batch(final_batch, &output_dir, &catalog).await;
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
                    flush_batch(batch, &output_dir, &catalog).await;
                }
            }
        }
    }

}


async fn flush_batch(batch: Vec<LogEvent>, output_dir: &str, pg_catalog: &PostgresIcebergCatalogManger) {
    let count = batch.len();
    info!(target: "ingestion", "Flushing micro-batch of {} events...", count);

    match events_record_batch(&batch) {
        Ok(record_batch) => {

            let writer = ParquetWriter::new(output_dir).expect("Failed to create output directory");

            match writer.write_batch(&record_batch) {
                Ok(file_path) => {
                    let metadata = std::fs::metadata(&file_path).unwrap();
                    let data_file = IcebergCatalogManager::create_data_file_entry(
                        file_path.to_str().unwrap(),
                        metadata.len(),
                        record_batch.num_rows() as u64
                    );

                    info!(
                        target: "ingestion",
                        "Committed DataFile manifest entry to Iceberg snapshot: path={}, records={}",
                        data_file.file_path(),
                        data_file.record_count()
                    );
                    // Execute atomic SQL transaction commit to Postgres SqlCatalog
                    if let Err(e) = pg_catalog.commit_data_file(data_file).await {
                        error!(target: "ingestion", "Failed to commit Iceberg snapshot transaction to Postgres: {}", e);
                    }
                }



                Err(err) => {
                    error!(target: "ingestion", "Failed to write Parquet file: {}", err);
                }
            }
        }
        Err(err) => {
            error!(target: "ingestion", "Failed to convert log events to Arrow RecordBatch: {}", err);
        }
    }

}