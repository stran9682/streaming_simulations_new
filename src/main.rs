use std::{env, str::FromStr, sync::Arc, time::Instant};

use iroh::{Endpoint, PublicKey, endpoint::presets, protocol::Router};
use streaming_simulations_new::{
    networking::{
        PacketData,
        iroh::{Iroh, SessionInfo},
    },
    packet_generators::generate_packets,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    println!("endpoint: {}", endpoint.id());

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
        let remote_endpoint = PublicKey::from_str(&args[1])?;
        iroh.make_request(endpoint, remote_endpoint).await?;
    }

    tokio::signal::ctrl_c().await?;

    Ok(())
}
