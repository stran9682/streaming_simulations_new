use std::{
    collections::HashSet,
    env,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use iroh::{Endpoint, PublicKey, endpoint::presets, protocol::Router};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip, TopicId, api::Event};
use rand::RngExt;
use streaming_simulations_new::{
    networking::{
        PacketData,
        iroh::{ConnectionTracker, Iroh, SessionInfo},
    },
    packet_generators::generate_packets,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::builder(presets::N0)
        .hooks(ConnectionTracker::new())
        .bind()
        .await?;

    endpoint.online().await;

    println!("endpoint: {}", endpoint.id());

    tokio::time::sleep(Duration::from_secs(rand::rng().random_range(1..20))).await;

    let clock = Instant::now();
    let session_info = SessionInfo {
        video_ssrc: rand::random(),
        audio_ssrc: rand::random(),
    };

    let (sender, receiver) = tokio::sync::broadcast::channel::<PacketData>(500);

    let iroh = Arc::new(Iroh::new(receiver, clock, session_info));
    generate_packets(clock, sender);

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .accept(b"coal", Arc::clone(&iroh))
        .spawn();

    let mut topic_bytes = [0u8; 32];
    topic_bytes[..4].copy_from_slice(b"coal");
    let topic_id = TopicId::from_bytes(topic_bytes);

    let args: Vec<String> = env::args().collect();
    let (send, mut recv) = if args.len() > 1 {
        let peers = vec![PublicKey::from_str(&args[1])?];
        gossip.subscribe_and_join(topic_id, peers).await?.split()
    } else {
        gossip.subscribe(topic_id, vec![]).await?.split()
    };

    send.broadcast(endpoint.id().to_string().into()).await?;

    let mut connections = HashSet::<PublicKey>::new();
    while let Some(event) = recv.next().await {
        match event? {
            Event::Received(message) => {
                let pk = PublicKey::from_str(str::from_utf8(&message.content)?)?;
                if connections.contains(&pk) {
                    continue;
                }

                if let Err(e) = {
                    connections.insert(pk);
                    iroh.make_request(&endpoint, &pk).await
                } {
                    eprintln!("Failed to connect: {}", e);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
