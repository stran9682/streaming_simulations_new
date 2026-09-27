use std::{
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

use bytes::Bytes;
use iroh::endpoint::Connection;
use tokio::sync::mpsc::{self, Receiver};

use crate::networking::{
    H264_CLOCK_RATE, OPUS_CLOCK_RATE, Peer,
    rtcp::{PacketType, RTCPHeader, SenderReport, ntp_to_middle_32, system_time_to_ntp},
    rtp::RTPHeader,
    write_stats,
};

async fn packet_handler(
    mut rx: Receiver<(RTPHeader, Bytes)>,
    clock_rate: u32,
    clock: Instant,
    peer_data: Arc<Mutex<Peer>>,
) {
    while let Some((header, _bytes)) = rx.recv().await {
        let arrival_time =
            ((clock.elapsed().as_nanos() * clock_rate as u128) / 1_000_000_000) as u32;
        let difference = arrival_time.wrapping_sub(header.timestamp);

        match peer_data.lock() {
            Ok(mut peer) => {
                peer.update_reception_stats(difference, header);
            }
            Err(e) => {
                eprintln!("RTP receiver lock failure: {e}")
            }
        }
    }

    println!("Dropped");
}

pub async fn packet_receiver(
    connection: Connection,
    audio_peer: Arc<Mutex<Peer>>,
    video_peer: Arc<Mutex<Peer>>,
    peer_video_ssrc: u32,
    clock: Instant,
) {
    let (a_tx, a_rx) = mpsc::channel::<(RTPHeader, Bytes)>(100);
    let (v_tx, v_rx) = mpsc::channel::<(RTPHeader, Bytes)>(100);

    let peer = audio_peer.clone();
    tokio::spawn(async move { packet_handler(a_rx, OPUS_CLOCK_RATE as u32, clock, peer).await });

    let peer = video_peer.clone();
    tokio::spawn(async move { packet_handler(v_rx, H264_CLOCK_RATE as u32, clock, peer).await });

    let (stats_send, stats_recv) = mpsc::channel::<(super::PacketType, f64)>(100);
    tokio::spawn(async move {
        if let Err(e) = write_stats(stats_recv).await {
            eprintln!("Error occured attempting to write to file: {}", e);
        };
    });

    while let Ok(mut bytes) = connection.read_datagram().await {
        if bytes.len() >= 2 && (72..=95).contains(&(bytes[1] & 0x7F)) {
            while bytes.len() >= 4 {
                let header = RTCPHeader::deserialize(&mut bytes);

                if header.packet_type != PacketType::SenderReport {
                    break;
                }

                let sender_report = SenderReport::deserialize(&mut bytes, header.count);

                let last_sr_timestamp = ntp_to_middle_32(sender_report.ntp_time);

                let (peer, packet_type) = if sender_report.ssrc == peer_video_ssrc {
                    (video_peer.lock(), super::PacketType::Video)
                } else {
                    (audio_peer.lock(), super::PacketType::Audio)
                };

                match peer {
                    Ok(mut peer) => {
                        peer.update_last_sr_timestamp(last_sr_timestamp);
                    }
                    Err(e) => {
                        eprintln!("RTCP Lock failure: {}", e);
                    }
                }

                // Determining our RTT from the RR
                let arrival_ntp_middle_32 = ntp_to_middle_32(system_time_to_ntp(SystemTime::now()));

                for report in &sender_report.reports {
                    if report.last_sr_timestamp == 0 || report.delay_since_last_sr == 0 {
                        continue;
                    }

                    let rtt = arrival_ntp_middle_32
                        - report.last_sr_timestamp
                        - report.delay_since_last_sr;
                    let rtt_ms = (rtt as f64 * 1000.0) / 65536.0;

                    println!("A - DLSR - LSR: {} ({:.2} ms)\n", rtt, rtt_ms);

                    if let Err(e) = stats_send.try_send((packet_type, rtt_ms)) {
                        eprintln!("Couldn't write stat, Reason: {}", e)
                    };
                }
            }
        } else {
            let header = RTPHeader::deserialize(&mut bytes);

            let tx = if header.ssrc == peer_video_ssrc {
                &v_tx
            } else {
                &a_tx
            };

            if let Err(e) = tx.send((header, bytes)).await {
                eprintln!("RTP receiver failure: {}", e);
                break;
            }
        }
    }
}
