use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use iroh::endpoint::Connection;
use tokio::sync::mpsc::{self, Receiver};

use crate::networking::{
    Peer,
    rtcp::{PacketType, RTCPHeader, SenderReport},
    rtp::RTPHeader,
};

async fn packet_handler(
    mut rx: Receiver<(RTPHeader, Bytes)>,
    clock_rate: u32,
    clock: Instant,
    peer_data: Arc<Mutex<Peer>>,
) {
    while let Some((header, _bytes)) = rx.recv().await {
        let arrival_time = clock.elapsed();
        let arrival_time = arrival_time.as_millis() as u32 * (clock_rate / 1000);
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
    video_ssrc: u32,
    clock: Instant,
) {
    let (a_tx, a_rx) = mpsc::channel::<(RTPHeader, Bytes)>(100);
    let (v_tx, v_rx) = mpsc::channel::<(RTPHeader, Bytes)>(100);

    let peer = audio_peer.clone();
    tokio::spawn(async move { packet_handler(a_rx, 48_000, clock, peer).await });

    let peer = video_peer.clone();
    tokio::spawn(async move { packet_handler(v_rx, 90_000, clock, peer).await });

    while let Ok(mut bytes) = connection.read_datagram().await {
        if bytes[1] & 0x7F >= 72 {
            for path in &connection.paths() {
                if let Some(rtt) = connection.rtt(path.id()) {
                    println!(
                        "path: {} \t is relay: {} \t is selected: {} \t remote: {} \t rtt: {:?}",
                        path.id(),
                        path.is_relay(),
                        path.is_selected(),
                        path.remote_addr(),
                        rtt.as_micros()
                    );
                }
            }

            while !bytes.is_empty() {
                let header = RTCPHeader::deserialize(&mut bytes);

                if header.packet_type == PacketType::SenderReport {
                    let sender_report = SenderReport::deserialize(&mut bytes, header.count);

                    let last_sr_timestamp = (sender_report.ntp_time >> 16 & 0xFFFFFFFF) as u32;

                    let peer = if sender_report.ssrc == video_ssrc {
                        video_peer.lock()
                    } else {
                        audio_peer.lock()
                    };

                    match peer {
                        Ok(mut peer) => {
                            peer.update_last_sr_timestamp(last_sr_timestamp);
                        }
                        Err(e) => {
                            eprintln!("RTCP Lock failure: {}", e);
                        }
                    }
                }
            }
        } else {
            let header = RTPHeader::deserialize(&mut bytes);

            let tx = if header.ssrc == video_ssrc {
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
