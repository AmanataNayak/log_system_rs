mod input;
mod parser;
mod std_input;
mod file_input;

use crate::input::LogInput;
use crate::std_input::StdinInput;
use crate::parser::EventParser;
use clap::Parser;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;
use common::log_system::log_ingestion_service_client::LogIngestionServiceClient;
use common::log_system::LogEvent as ProtoLogEvent;
use crate::file_input::FileInput;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the service emitting logs
    #[arg(short, long, env = "SERVICE_NAME")]
    service_name: String,

    /// gRPC Ingestion service url
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    ingestion_url: String,

    /// Optional file glob pattern (e.g. "data/logs/*.log"). If omitted, reads from STDIN.
    #[arg(short, long)]
    pub path: Option<String>,

    /// Poll interval in case reading from file
    #[arg(long, default_value = "500")]
    poll_interval: u64
}



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    // CLI arguments
    let args = Args::parse();

    // Initialize tracing logger for collector binary itself
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // Box<dyn InputSource> lets us decide at runtime whether we read from STDIN or Files.
    let input: Box<dyn LogInput> = match args.path {
        Some(glob_pattern) => Box::new(FileInput::new(glob_pattern, args.poll_interval)),
        None => Box::new(StdinInput)
    };

    // Bounded mpsc channel: buffers up to 1000 lines before applying backpressure
    let (input_tx, mut input_rx) = mpsc::channel::<String>(1000);
    let (grpc_tx, grpc_rx) = mpsc::channel::<ProtoLogEvent>(1000);

    let parser = EventParser::new(args.service_name);

    // Producer task: reads raw lines from stdin
    let input_task = tokio::spawn(async move {

        if let Err(e) = input.run(input_tx).await {
            tracing::error!("Input reader error: {}", e);
        }
    });

    // consumer task: parses raw lines into LogEvents and forwards ProtoLogEvents
    let consumer_task = tokio::spawn(async move {
       while let Some(raw_line) = input_rx.recv().await {
           let event = parser.parse(&raw_line);

           // Debug print: inspect parsed LogEvent fields
           info!(
               target: "collector",
               "[{}] [{}] {} (meta: {:?})",
               event.log_level,
               event.service_name,
               event.message,
               event.metadata
           );

           // Convert domain LogEvent to gRPC ProtoLogEvent and send to gRPC streamer
           let proto_event: ProtoLogEvent = event.into();
           if grpc_tx.send(proto_event).await.is_err() {
               tracing::warn!("gRPC channel closed, stopping log forwarding");
               break;
           }
       }
    });

    // gRPC client task: streams ProtoLogEvent to Ingestion Service
    let grpc_task = tokio::spawn(start_grpc_stream(args.ingestion_url, grpc_rx));

    // Wait for both tasks to shut down gracefully
    let _ = tokio::join!(input_task, consumer_task, grpc_task);
    Ok(())
}


pub async fn start_grpc_stream(ingestion_url: String, mut rx: mpsc::Receiver<ProtoLogEvent>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>{
    let mut client = LogIngestionServiceClient::connect(ingestion_url).await?;

    // Create an unbounded/bounded mpsc stream wrapper for Tonic
    let (stream_tx, stream_rx) = mpsc::channel(1000);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if stream_tx.send(event).await.is_err() {
                // ingestion server disconnected
                break;
            }
        }
    });

    let request = tonic::Request::new(ReceiverStream::new(stream_rx));
    let response = client.stream_logs(request).await?;

    info!(
        "Ingestion ack: success={}, count={}",
        response.get_ref().success,
        response.get_ref().count
    );

    Ok(())
}