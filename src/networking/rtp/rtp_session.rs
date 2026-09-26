use std::{
    sync::atomic::{AtomicU16, AtomicU32, Ordering::Relaxed},
    time::Instant,
};

#[derive(Debug)]
pub struct RTPSession {
    current_sequence_num: AtomicU16,
    packets_generated: AtomicU32,
    octets_sent: AtomicU32,

    pub ssrc: u32,
    pub clock_rate: u64,
    pub clock: Instant,
}

impl RTPSession {
    pub fn new(ssrc: u32, clock_rate: u64, clock: Instant) -> Self {
        Self {
            octets_sent: AtomicU32::new(0),
            current_sequence_num: AtomicU16::new(0),
            packets_generated: AtomicU32::new(0),
            ssrc,
            clock_rate,
            clock,
        }
    }

    pub fn get_packet(&self, marker: bool, timestamp: u32, packet_length: u32) -> super::RTPHeader {
        let current_sequence_num = self.current_sequence_num.fetch_add(1, Relaxed);
        self.packets_generated.fetch_add(1, Relaxed);
        self.octets_sent.fetch_add(packet_length, Relaxed);

        super::RTPHeader {
            version: 2,
            padding: false,
            extension: false,
            marker,
            payload_type: 0,
            sequence_number: current_sequence_num,
            timestamp,
            ssrc: self.ssrc,
            // csrc:
        }
    }

    pub fn get_num_packets_generated(&self) -> u32 {
        self.packets_generated.load(Relaxed)
    }

    pub fn get_num_octets_sent(&self) -> u32 {
        self.octets_sent.load(Relaxed)
    }

    pub fn get_peer_min_window(&self) {}
}
