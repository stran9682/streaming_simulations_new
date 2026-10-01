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

use crate::networking::rtcp::ReceptionReport;

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

    audio_stats_writer
        .write(b"rtt,jitter,total lost,fraction lost")
        .await?;
    video_stats_writer
        .write(b"rtt,jitter,total lost,fraction lost")
        .await?;

    while let Some(stats) = rx.recv().await {
        let writer = match stats.packet_type {
            PacketType::Audio => &mut audio_stats_writer,
            PacketType::Video => &mut video_stats_writer,
        };

        let report = stats.report;

        let rtt =
            stats.arrival_ntp_middle_32 - report.last_sr_timestamp - report.delay_since_last_sr;
        let rtt_ms = (rtt as f64 * 1000.0) / 65536.0;

        let fraction_lost = report.fraction_lost as f64 / 256.0;

        if cfg!(debug_assertions) {
            println!("A - DLSR - LSR: {} ({:.2} ms)\n", rtt, rtt_ms);
        }

        let jitter = match stats.packet_type {
            PacketType::Audio => report.jitter as f64 / 48_000.0,
            PacketType::Video => report.jitter as f64 / 90_000.0,
        };

        writer
            .write(
                &format!(
                    "{},{},{},{}\n",
                    rtt, jitter, report.total_lost, fraction_lost
                )
                .into_bytes(),
            )
            .await?;
    }

    audio_stats_writer.flush().await?;
    video_stats_writer.flush().await?;

    Ok(())
}

pub struct Stats {
    pub packet_type: PacketType,
    pub report: ReceptionReport,
    pub arrival_ntp_middle_32: u32,
}
