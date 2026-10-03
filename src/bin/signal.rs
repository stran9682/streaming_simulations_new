use std::{collections::HashSet, str::FromStr};

use iroh::{Endpoint, PublicKey, endpoint::presets, protocol::Router};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip, TopicId, api::Event};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    println!("endpoint: {}", endpoint.id());

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let mut topic_bytes = [0u8; 32];
    topic_bytes[..4].copy_from_slice(b"coal");
    let topic_id = TopicId::from_bytes(topic_bytes);

    let (send, mut recv) = gossip.subscribe(topic_id, vec![]).await?.split();

    send.broadcast(endpoint.id().to_string().into()).await?;

    let mut peers = HashSet::<PublicKey>::new();
    while let Some(event) = recv.next().await {
        match event? {
            Event::Received(message) => {
                let pk = PublicKey::from_str(str::from_utf8(&message.content)?)?;

                if peers.contains(&pk) {
                    continue;
                } else {
                    println!("Peer {} has joined", pk);
                    peers.insert(pk);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
