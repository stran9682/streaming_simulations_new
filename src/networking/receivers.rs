use std::{
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

use iroh::endpoint::Connection;
use tokio::sync::mpsc::{self};

use crate::networking::{
    H264_CLOCK_RATE, OPUS_CLOCK_RATE, Peer,
    rtcp::{PacketType, RTCPHeader, SenderReport, ntp_to_middle_32, system_time_to_ntp},
    rtp::RTPHeader,
    write_stats,
};

pub async fn packet_receiver(
    connection: Connection,
    audio_peer: Arc<Mutex<Peer>>,
    video_peer: Arc<Mutex<Peer>>,
    peer_video_ssrc: u32,
    clock: Instant,
) {
    let (stats_send, stats_recv) = mpsc::channel::<(super::PacketType, f64)>(100);
    tokio::spawn(async move {
        if let Err(e) = write_stats(stats_recv, peer_video_ssrc).await {
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

                    if cfg!(debug_assertions) {
                        println!("A - DLSR - LSR: {} ({:.2} ms)\n", rtt, rtt_ms);
                    }

                    if let Err(e) = stats_send.try_send((packet_type, rtt_ms)) {
                        eprintln!("Couldn't write stat, Reason: {}", e)
                    };
                }
            }
        } else {
            let header = RTPHeader::deserialize(&mut bytes);

            let (peer_lock, clock_rate) = if header.ssrc == peer_video_ssrc {
                (&video_peer, H264_CLOCK_RATE as u32)
            } else {
                (&audio_peer, OPUS_CLOCK_RATE as u32)
            };

            let arrival_time =
                ((clock.elapsed().as_nanos() * clock_rate as u128) / 1_000_000_000) as u32;
            let difference = arrival_time.wrapping_sub(header.timestamp);

            if let Ok(mut peer) = peer_lock.lock() {
                peer.update_reception_stats(difference, header);
            }
        }
    }
}
