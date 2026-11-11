use datafusion::prelude::SessionContext;
use common::pubsub::pub_sub_service_client::PubSubServiceClient;
use common::pubsub::{CreateSubscriptionRequest, StreamingPullRequest};
use tokio_stream::wrappers::ReceiverStream;
use iceberg::Catalog;
use tokio::sync::mpsc;
use std::sync::Arc;
use iceberg_datafusion::IcebergCatalogProvider;
use tracing::info;
use common::event::TableCommitEvent;

pub async fn start_invalidation_listener(topic: String, subscription: String, ctx: SessionContext, catalog: Arc<dyn Catalog>) -> anyhow::Result<()> {
    const ADDR: &str = "http://127.0.0.1:50052";

    let mut client = PubSubServiceClient::connect(ADDR).await?;

    // Ensure subscription exits
    let _ = client
        .create_subscription(CreateSubscriptionRequest {
            topic: topic.clone(),
            subscription: subscription.clone(),
            ack_deadline_secs: 10,
            batch_size: Some(10),
            max_outstanding_messages: Some(100),
            message_ttl_secs: None,
            dead_letter_policy: None,
        }).await;

    // Open bidirectional stream
    let (tx, rx) = mpsc::channel::<StreamingPullRequest>(32);

    // Initial handshake frame
    tx.send(StreamingPullRequest {
        subscription_name: subscription.clone(),
        topic_name: topic.clone(),
        ack_ids: vec![],
        nack_ids: vec![],
        max_messages: Some(10),
    })
        .await?;

    let response_stream = client.streaming_pull(ReceiverStream::new(rx)).await?;
    let mut stream = response_stream.into_inner();

    tokio::spawn(async move {
        info!(target: "query_engine", "Connected to pub_sub_rs StreamingPull loop on port 50052");

        while let Ok(Some(resp)) = stream.message().await {
            let mut ack_ids = Vec::new();

            for msg in resp.messages {
                if let Ok(event) = serde_json::from_slice::<TableCommitEvent>(&msg.payload) {
                    info!(
                        target: "query_engine",
                        "Received commit invalidation for {}.{} (snapshot: {}). Refreshing DataFusion catalog...",
                        event.namespace, event.table_name, event.snapshot_id
                    );

                    if let Ok(provider) = IcebergCatalogProvider::try_new(catalog.clone()).await {
                        ctx.register_catalog("iceberg", Arc::new(provider));
                        info!(target: "query_engine", "Successfully invalidated & updated IcebergCatalogProvider!");
                    }
                }

                ack_ids.push(msg.id);
            }

            // Acknowledge recived message back over gRPC stream
            if !ack_ids.is_empty() {
                let _ = tx.send(
                    StreamingPullRequest {
                        subscription_name: subscription.clone(),
                        topic_name: topic.clone(),
                        ack_ids,
                        nack_ids: vec![],
                        max_messages: Some(10),
                    }
                ).await;
            }
        }
    });
    Ok(())
}