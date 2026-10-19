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
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let file = File::open("input.h264")?;
    let reader = BufReader::with_capacity(256 * 1024, file);

    let mut video_reader = H26xSampleReader::new(reader, 1024 * 1024, false);

    while let Ok(sample) = video_reader.next_sample() {
        let elapsed_micros = clock.elapsed().as_micros() as u64;
        let elapsed = ((elapsed_micros * 9) / 100) as u32;

        let packet_data = PacketData {
            packet_type: PacketType::Video,
            data: sample.data,
            timestamp: elapsed,
        };

        if let Err(e) = tx.send(packet_data) {
            if tx.receiver_count() > 0 {
                bail!("Error occured {}", e);
            }
        };

        if sample.timed {
            tick.tick().await;
        }
    }

    Ok(())
}

async fn generate_audio_sample(tx: Sender<PacketData>, clock: Instant) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let file = File::open("input.ogg")?;
    let reader = BufReader::with_capacity(128 * 1024, file);

    let (mut audio_reader, _) = OggReader::new(reader, true).expect("Cannot read OGG file");

    while let Ok((page_data, _)) = audio_reader.parse_next_page() {
        let elapsed_micros = clock.elapsed().as_micros() as u64;
        let elapsed = ((elapsed_micros * 48) / 1000) as u32;

        let packet_data = PacketData {
            packet_type: PacketType::Audio,
            data: page_data.freeze(),
            timestamp: elapsed,
        };

        if let Err(e) = tx.send(packet_data) {
            if tx.receiver_count() > 0 {
                bail!("Error occured {}", e);
            }
        };

        tick.tick().await;
    }

    Ok(())
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
            eprintln!("Couldn't generate audio samples: {}", e);
        }
    });
}
