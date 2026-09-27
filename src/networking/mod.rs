use bytes::Bytes;

pub mod iroh;
pub mod peer;
pub mod receivers;
pub mod rtcp;
pub mod rtp;
pub mod senders;

pub use peer::Peer;

use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

#[derive(Clone, Copy)]
pub enum PacketType {
    Video,
    Audio,
}

#[derive(Clone)]
pub struct PacketData {
    pub packet_type: PacketType,
    pub data: Bytes,
    pub timestamp: u32,
}

const OPUS_CLOCK_RATE: u64 = 48_000;
const H264_CLOCK_RATE: u64 = 90_000;

pub async fn write_stats(mut rx: mpsc::Receiver<(PacketType, f64)>) -> anyhow::Result<()> {
    let audio_stats = File::create("audio-stats.csv").await?;
    let mut audio_stats_writer = BufWriter::new(audio_stats);

    let video_stats = File::create("video-stats.csv").await?;
    let mut video_stats_writer = BufWriter::new(video_stats);

    while let Some((packet_type, rtt)) = rx.recv().await {
        let writer = match packet_type {
            PacketType::Audio => &mut audio_stats_writer,
            PacketType::Video => &mut video_stats_writer,
        };

        writer.write(&format!("{rtt}\n").into_bytes()).await?;
    }

    Ok(())
}
