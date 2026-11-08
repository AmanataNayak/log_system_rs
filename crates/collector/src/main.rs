mod input;
mod parser;

use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use crate::input::{LogInput, StdinInput};
use crate::parser::EventParser;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    // Name of the service emitting logs
    #[arg(short, long, env = "SERVICE_NAME")]
    service_name: String,
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

    // Bounded mpsc channel: buffers up to 1000 lines before applying backpressure
    let (tx, mut rx) = mpsc::channel::<String>(1000);
    let parser = EventParser::new(args.service_name);

    // Producer task
    let input_task = tokio::spawn(async move {
        let input = StdinInput;
        if let Err(e) = input.run(tx).await {
            tracing::error!("Input reader error: {}", e);
        }
    });

    // consumer task
    let consumer_task = tokio::spawn(async move {
       while let Some(raw_line) = rx.recv().await {
           let event = parser.parse(&raw_line);

           // Debug print: inspect parsed LogEvent fields
           info!(
               target: "collector",
               "[{}] [{}] {} (meta: {:?})",
               event.log_level,
               event.service_name,
               event.message,
               event.metadata
           )
       }
    });

    // Wait for both tasks to shut down gracefully
    let _ = tokio::join!(input_task, consumer_task);
    Ok(())
}
