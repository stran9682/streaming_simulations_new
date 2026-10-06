use std::{
    collections::HashSet,
    env,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use iroh::{Endpoint, EndpointId, PublicKey, endpoint::presets, protocol::Router};
use rand::RngExt;
use streaming_simulations_new::{
    networking::{
        PacketData,
        iroh::{ConnectionTracker, Iroh, SessionInfo},
    },
    packet_generators::generate_packets,
};

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

    let _router = Router::builder(endpoint.clone())
        .accept(b"coal", Arc::clone(&iroh))
        .spawn();

    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        let signaling_addr = PublicKey::from_str(&args[1])?;

        let conn = endpoint.connect(signaling_addr, b"discovery").await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        send.write_all(endpoint.id().as_bytes()).await?;

        let bytes = recv.read_to_end(usize::MAX).await?;

        let remote_endpoints: HashSet<EndpointId> = serde_json::from_slice(&bytes)?;

        for remote_endpoint in remote_endpoints {
            if let Err(e) = iroh.make_request(&endpoint, &remote_endpoint).await {
                eprintln!("Couldn't connect to {}, {}", remote_endpoint, e);
            };
        }
    }

    tokio::signal::ctrl_c().await?;

    Ok(())
}
