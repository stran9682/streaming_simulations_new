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

    let mut buf = BytesMut::with_capacity(payload_size);

    'receiver: while let Ok(packet_data) = bytes_receiver.recv().await {
        let packets = match packet_data.packet_type {
            Audio => {
                let header =
                    audio.get_packet(false, packet_data.timestamp, packet_data.data.len() as u32);

                header.serialize(&mut buf);

                buf.extend_from_slice(&packet_data.data);

                let packet = buf.split().freeze();

                vec![packet]
            }
            Video => {
                let mut payloads: Vec<Bytes> = Vec::new();
                let bytes = &packet_data.data;

                let mut nalu_data_index = 1;
                let nalu_data_length = bytes.len() - nalu_data_index;
                let mut nalu_data_remaining = nalu_data_length;

                let nalu_nri = bytes[0] & 0x60;
                let nalu_type = bytes[0] & 0x1F;

                if bytes.len() <= payload_size {
                    let header = video.get_packet(true, packet_data.timestamp, bytes.len() as u32);

                    header.serialize(&mut buf);
                    buf.extend_from_slice(&packet_data.data);

                    let packet = buf.split().freeze();

                    payloads.push(packet);
                } else {
                    while nalu_data_remaining > 0 {
                        let current_fragment_size = min(payload_size, nalu_data_remaining);

                        let header = video.get_packet(
                            payload_size >= nalu_data_remaining, // VERY last one
                            packet_data.timestamp,
                            current_fragment_size as u32 + 2,
                        );

                        header.serialize(&mut buf);

                        /*
                            +---------------+---------------+
                            |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
                            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                            |F|NRI|  Type   |S|E|R|  Type   |
                            +---------------+---------------+

                            F           : should always be 0
                            NRI         : Essentialy level of importance, needs to be copied
                            Type (1)    : Type of header. 28 To indicate this is a fragment
                            S(tart)     : indicates this is the start
                            E(nd)       : indicates this is the end
                            R(eserved)  : always 0
                            Type (2)    : Kind of payload, needs to be copied

                            Original header needs to be reconstructed!
                        */

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
                }

                payloads
            }
        };

        // if let Err(e) = connection.send_many_datagrams(&packets) {
        //     eprintln!("Send datagram error: {}", e);
        //     break 'receiver;
        // }

        for packet in packets {
            if let Err(e) = connection.send_datagram_wait(packet).await {
                eprintln!("Send datagram error: {}", e);
                break 'receiver;
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
            ((elapsed.as_nanos() * rtp_session.clock_rate as u128) / 1_000_000_000) as u32;

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

        let body = sender_report.serialize();
        let mut packet = BytesMut::with_capacity(4 + body.len());
        packet.put(header.serialize());
        packet.put(body);

        let packet = packet.freeze();

        if let Err(e) = connection.send_datagram(packet) {
            eprintln!("Failed to send RTCP to {}: {}", connection.remote_id(), e);
            break;
        }
    }
}
