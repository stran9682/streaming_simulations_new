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

pub async fn write_stats(mut rx: mpsc::Receiver<Stats>, ssrc: u32) -> anyhow::Result<()> {
    let audio_stats = File::create(format!("audio-stats-{ssrc}.csv")).await?;
    let mut audio_stats_writer = BufWriter::with_capacity(256, audio_stats);

    let video_stats = File::create(format!("video-stats-{ssrc}.csv")).await?;
    let mut video_stats_writer = BufWriter::with_capacity(256, video_stats);

    while let Some(stats) = rx.recv().await {
        let writer = match stats.packet_type {
            PacketType::Audio => &mut audio_stats_writer,
            PacketType::Video => &mut video_stats_writer,
        };

        writer
            .write(
                &format!("{},{},{}\n", stats.rtt, stats.fraction_lost, stats.jitter).into_bytes(),
            )
            .await?;
    }

    audio_stats_writer.flush().await?;
    video_stats_writer.flush().await?;

    Ok(())
}

pub struct Stats {
    pub packet_type: PacketType,
    pub rtt: f64,
    pub fraction_lost: f64,
    pub jitter: u32,
}
