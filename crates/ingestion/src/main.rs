mod service;
mod buffer;
mod arrow;
mod parquet_writer;
mod iceberg_catalog;
mod sql_catalog;
mod pubshiler;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use clap::builder::Str;
use tokio::sync::mpsc;
use clap::{arg, Parser};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use crate::service::{run_batch_flusher, IngestionServer};
use tracing::info;
use tonic::transport::Server;
use common::log_system::log_ingestion_service_server::LogIngestionServiceServer;
use common::log_system::LogEvent;
use crate::buffer::BatchConfig;
use crate::sql_catalog::PostgresIcebergCatalogManger;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Port for gRPC Ingestion server to bind to
    #[arg(short, long, env = "PORT", default_value = "50051")]
    port: u16,

    #[arg(short, long, default_value = "1000")]
    batch_size: usize,

    #[arg(short, long, default_value = "5")]
    flush_interval: u64,

    #[arg(short, long, default_value = "postgres://postgres:postgres@localhost:5432/iceberg_catalog")]
    database_url: String,

    #[arg(short, long, default_value = "data/iceberg_warehouse")]
    warehouse_dir: String,

    #[arg(short, long, default_value = "log_db")]
    namespace: String,

    #[arg(short, long, default_value = "log_events")]
    table: String,

}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    let max_batch_size = args.batch_size;
    let max_flush_interval = args.flush_interval;
    let warehouse_dir = args.warehouse_dir;
    let postgres_url = args.database_url;
    let namespace = args.namespace;
    let table = args.table;

    let (tx, rx) = mpsc::channel::<LogEvent>(1000);
    let config = BatchConfig{
        max_batch_size,
        max_flush_interval: Duration::from_secs(max_flush_interval)
    };

    // Database & Catalog Configuration
    let catalog_manager = Arc::new(
        PostgresIcebergCatalogManger::new(&postgres_url, &warehouse_dir, &namespace, &table)
            .await
            .map_err(anyhow::Error::from_boxed)?
    );

    // Spawn background micro-batching flusher task
    let catalog_clone = Arc::clone(&catalog_manager);
    let output_dir = format!("{warehouse_dir}/{namespace}/{table}/data");
    tokio::spawn(run_batch_flusher(rx, config, output_dir, catalog_clone));

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
    let service = IngestionServer::new(tx);

    info!(target: "ingestion", "Starting Ingestion gRPC server on {}", addr);

    Server::builder()
        .add_service(LogIngestionServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
