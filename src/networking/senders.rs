use std::{
    cmp::min,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use bytes::{BufMut, Bytes, BytesMut};
use iroh::endpoint::Connection;
use rand::RngExt;
use tokio::{sync::broadcast::Receiver, time::sleep};

use crate::networking::{
    PacketData,
    PacketType::{Audio, Video},
    Peer,
    rtcp::{PacketType, RTCPHeader, SenderReport, system_time_to_ntp},
    rtp::RTPSession,
};

pub async fn send(
    connection: Connection,
    audio: Arc<RTPSession>,
    video: Arc<RTPSession>,
    mut bytes_receiver: Receiver<PacketData>,
) {
    let payload_size = 1100;
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
    let mut payloads: Vec<Bytes> = Vec::with_capacity(64);

    'receiver: loop {
        let packet_data = match bytes_receiver.recv().await {
            Ok(data) => data,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                if cfg!(debug_assertions) {
                    eprintln!("Sender lagged by {} packets", skipped);
                }
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                break 'receiver;
            }
        };

        match packet_data.packet_type {
            Audio => {
                let header =
                    audio.get_packet(false, packet_data.timestamp, packet_data.data.len() as u32);

                let required_len = 12 + packet_data.data.len();
                if buf.capacity() < required_len {
                    buf.reserve(CHUNK_SIZE.max(required_len));
                }

                header.serialize(&mut buf);
                buf.extend_from_slice(&packet_data.data);
                let packet = buf.split().freeze();

                if let Err(e) = connection.send_datagram(packet) {
                    eprintln!("Send datagram error: {}", e);
                    break 'receiver;
                }
            }
            Video => {
                payloads.clear();
                let bytes = &packet_data.data;
                if bytes.is_empty() {
                    continue;
                }

                let mut nalu_data_index = 1;
                let nalu_data_length = bytes.len() - nalu_data_index;
                let mut nalu_data_remaining = nalu_data_length;

                let nalu_nri = bytes[0] & 0x60;
                let nalu_type = bytes[0] & 0x1F;

                if bytes.len() <= payload_size {
                    let header = video.get_packet(true, packet_data.timestamp, bytes.len() as u32);

                    let required_len = 12 + bytes.len();
                    if buf.capacity() < required_len {
                        buf.reserve(CHUNK_SIZE.max(required_len));
                    }

                    header.serialize(&mut buf);
                    buf.extend_from_slice(bytes);
                    let packet = buf.split().freeze();

                    if let Err(e) = connection.send_datagram(packet) {
                        eprintln!("Send datagram error: {}", e);
                        break 'receiver;
                    }
                } else {
                    let num_frags = (nalu_data_remaining + payload_size - 1) / payload_size;
                    if payloads.capacity() < num_frags {
                        payloads.reserve(num_frags - payloads.capacity());
                    }

                    while nalu_data_remaining > 0 {
                        let current_fragment_size = min(payload_size, nalu_data_remaining);

                        let header = video.get_packet(
                            payload_size >= nalu_data_remaining, // VERY last one
                            packet_data.timestamp,
                            current_fragment_size as u32 + 2,
                        );

                        let required_len = 12 + 2 + current_fragment_size;
                        if buf.capacity() < required_len {
                            buf.reserve(CHUNK_SIZE.max(required_len));
                        }

                        header.serialize(&mut buf);

                        let b0 = 28 | nalu_nri; // 28 to indicate FU-A packet type
                        buf.put_u8(b0);

                        let mut b1 = nalu_type;
                        if nalu_data_remaining == nalu_data_length {
                            // Set start bit
                            b1 |= 1 << 7;
                        } else if nalu_data_remaining - current_fragment_size == 0 {
                            // Set end bit
                            b1 |= 1 << 6;
                        }
                        buf.put_u8(b1);

                        buf.put_slice(
                            &bytes[nalu_data_index..nalu_data_index + current_fragment_size],
                        );

                        nalu_data_remaining -= current_fragment_size;
                        nalu_data_index += current_fragment_size;

                        let packet = buf.split().freeze();
                        payloads.push(packet);
                    }

                    if let Err(e) = connection.send_many_datagrams(&payloads) {
                        eprintln!("Send datagram error: {}", e);
                        break 'receiver;
                    }
                }
            }
        }
    }
}

pub async fn send_rtcp(
    rtp_session: Arc<RTPSession>,
    connection: Connection,
    peer: Arc<Mutex<Peer>>,
) {
    let mut first_packet = true;
    let mut packet_buf = BytesMut::with_capacity(128);

    loop {
        let mut interval = 5.0;

        interval = {
            let mut rng = rand::rng();
            rng.random_range(0.5..=1.5) * interval
        };

        if first_packet {
            interval *= 0.5;
            first_packet = false;
        }

        sleep(Duration::from_secs_f64(interval)).await;

        let ntp = system_time_to_ntp(SystemTime::now());
        let elapsed = rtp_session.clock.elapsed();
        let rtp_time =
            ((elapsed.as_micros() as u64 * rtp_session.clock_rate) / 1_000_000) as u32;

        let reports = peer
            .lock()
            .map_or_else(|_| vec![], |mut p| vec![p.reception_report()]);

        let sender_report = SenderReport {
            ssrc: rtp_session.ssrc,
            ntp_time: ntp,
            rtp_time,
            packet_count: rtp_session.get_num_packets_generated(),
            octet_count: rtp_session.get_num_octets_sent(),
            reports,
        };

        let header = RTCPHeader {
            padding: false,
            packet_type: PacketType::SenderReport,
            count: sender_report.reports.len() as u8,
            length: sender_report.length(),
        };

        packet_buf.clear();
        let required_len = 4 + 24 + sender_report.reports.len() * 24;
        if packet_buf.capacity() < required_len {
            packet_buf.reserve(required_len - packet_buf.capacity());
        }

        header.serialize_into(&mut packet_buf);
        sender_report.serialize_into(&mut packet_buf);

        let packet = packet_buf.split().freeze();

        if let Err(e) = connection.send_datagram(packet) {
            eprintln!("Failed to send RTCP to {}: {}", connection.remote_id(), e);
            break;
        }
    }
}
