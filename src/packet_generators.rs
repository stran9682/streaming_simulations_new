use std::time::{Duration, Instant};

use anyhow::bail;
use bytes::Bytes;
use tokio::{fs::File, io::AsyncReadExt, sync::broadcast::Sender};

use crate::networking::{PacketData, PacketType};

async fn generate_video_frame(tx: Sender<PacketData>, clock: Instant) -> anyhow::Result<()> {
    loop {
        let mut file = File::open("input.h264").await?;

        loop {
            let mut avcc_start_code: [u8; 4] = [0; 4];

            if file.read_exact(&mut avcc_start_code).await.is_err() {
                break;
            }

            let nal_unit_length = u32::from_be_bytes(avcc_start_code) as usize;

            let mut buffer = vec![0; nal_unit_length];
            let bytes_read = file.read_buf(&mut buffer).await?;

            if bytes_read == 0 {
                break;
            }

            let elapsed = ((clock.elapsed().as_nanos() * 90_000) / 1_000_000_000) as u32;

            let packet_data = PacketData {
                packet_type: PacketType::Video,
                data: Bytes::copy_from_slice(&buffer[..bytes_read]),
                timestamp: elapsed,
            };

            if let Err(e) = tx.send(packet_data) {
                bail!("Error occured {}", e)
            };

            tokio::time::sleep(Duration::from_secs_f32(1.0 / 30.0)).await;
        }
    }
}

async fn generate_audio_sample(tx: Sender<PacketData>, clock: Instant) -> anyhow::Result<()> {
    loop {
        let mut file = File::open("input.ogg").await?;
        let mut opus_data = Vec::new();
        file.read_to_end(&mut opus_data).await?;

        let packets = parse_ogg_opus_packets(&opus_data)?;

        for packet in packets {
            let elapsed = ((clock.elapsed().as_nanos() * 48_000) / 1_000_000_000) as u32;

            let packet_data = PacketData {
                packet_type: PacketType::Audio,
                data: Bytes::copy_from_slice(&packet),
                timestamp: elapsed,
            };

            if let Err(e) = tx.send(packet_data) {
                bail!("Error occured {}", e)
            };

            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn parse_ogg_opus_packets(file: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut offset = 0;
    let mut packets = Vec::new();

    while offset + 27 < file.len() {
        if &file[offset..offset + 4] != b"OggS" {
            break;
        }

        let page_segments = file[offset + 26] as usize;
        let segment_table_start = offset + 27;
        let segment_table_end = segment_table_start + page_segments;

        if segment_table_end > file.len() {
            break;
        }

        let mut packet_start = segment_table_end;
        for segment_size in &file[segment_table_start..segment_table_end] {
            let size = *segment_size as usize;
            let packet_end = packet_start + size;

            if packet_end > file.len() {
                break;
            }

            let packet = &file[packet_start..packet_end];
            if !packet.starts_with(b"OpusHead") && !packet.starts_with(b"OpusTags") {
                packets.push(packet.to_vec());
            }

            packet_start = packet_end;
        }

        offset = packet_start;
    }

    Ok(packets)
}

pub fn generate_packets(clock: Instant, sender: Sender<PacketData>) {
    let sender_copy = sender.clone();
    tokio::spawn(async move { generate_video_frame(sender_copy, clock).await });

    tokio::spawn(async move { generate_audio_sample(sender, clock).await });
}
