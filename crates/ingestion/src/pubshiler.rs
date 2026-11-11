use std::collections::HashMap;
use common::event::TableCommitEvent;
use tonic::transport::Channel;
use tracing::info;

use common::pubsub::pub_sub_service_client::PubSubServiceClient;
use common::pubsub::{CreateTopicRequest, PublishRequest};


#[derive(Clone)]
pub struct InvalidationPublisher {
    client: PubSubServiceClient<Channel>,
    topic: String
}


impl InvalidationPublisher {
    pub async fn connect(topic: &str) -> anyhow::Result<Self> {
        const ADDR: &str = "http://127.0.0.1:50052";
        let mut client = PubSubServiceClient::connect(ADDR).await?;

        // ensure topic exist
        let _ = client
            .create_topic(CreateTopicRequest{
                topic: topic.to_string()
            })
            .await;

        Ok(Self {
            client,
            topic: topic.to_string()
        })
    }

    pub async fn publish_commit(&self, event: &TableCommitEvent) -> anyhow::Result<String> {
        let payload = serde_json::to_vec(event)?;

        let request = PublishRequest {
            topic: self.topic.clone(),
            payload,
            attributes: HashMap::new()
        };

        let mut client = self.client.clone();
        let response = client.publish(request).await?;
        let message_id = response.into_inner().message_id;

        info!(
            target: "ingestion",
            "Published commit event to pub_sub_rs [msg_id: {}] for {}.{}",
            message_id, event.namespace, event.table_name
        );

        Ok(message_id)
    }

}