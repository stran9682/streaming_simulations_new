use std::{
    fs::File,
    io::BufReader,
    time::{Duration, Instant},
};

use anyhow::bail;
use rtc::media::io::{h26x_reader::H26xSampleReader, ogg_reader::OggReader};
use tokio::sync::broadcast::Sender;

use crate::networking::{PacketData, PacketType};

async fn generate_video_frame(tx: Sender<PacketData>, clock: Instant) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(41));
    loop {
        let file = File::open("input.h264")?;
        let reader = BufReader::new(file);

        let mut video_reader = H26xSampleReader::new(reader, 1024 * 1024, false);

        while let Ok(sample) = video_reader.next_sample() {
            let elapsed = ((clock.elapsed().as_nanos() * 90_000) / 1_000_000_000) as u32;

            let packet_data = PacketData {
                packet_type: PacketType::Video,
                data: sample.data,
                timestamp: elapsed,
            };

            if let Err(e) = tx.send(packet_data) {
                bail!("Error occured {}", e)
            };

            if sample.timed {
                tick.tick().await;
            }
        }
    }
}

async fn generate_audio_sample(tx: Sender<PacketData>, clock: Instant) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    loop {
        let file = File::open("input.ogg")?;
        let reader = BufReader::new(file);

        let (mut audio_reader, _) = OggReader::new(reader, true).expect("Cannot read OGG file");

        while let Ok((page_data, _)) = audio_reader.parse_next_page() {
            let elapsed = ((clock.elapsed().as_nanos() * 48_000) / 1_000_000_000) as u32;

            let packet_data = PacketData {
                packet_type: PacketType::Audio,
                data: page_data.freeze(),
                timestamp: elapsed,
            };

            if let Err(e) = tx.send(packet_data) {
                bail!("Error occured {}", e)
            };

            tick.tick().await;
        }
    }
}

pub fn generate_packets(clock: Instant, sender: Sender<PacketData>) {
    let sender_copy = sender.clone();
    tokio::spawn(async move {
        if let Err(e) = generate_video_frame(sender_copy, clock).await {
            eprintln!("Couldn't generate video frames: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = generate_audio_sample(sender, clock).await {
            eprintln!("Couldn't generate video frames: {}", e);
        }
    });
}
