use std::{collections::VecDeque, time::Instant};

use crate::networking::{rtcp::ReceptionReport, rtp::RTPHeader};

static WINDOW_SIZE: usize = 50;

#[derive(Debug)]
pub struct Peer {
    pub ssrc: u32,

    ///  variance in arrival time
    pub jitter: u32,

    /// highest sequence number currently received from this peer         
    pub max_sequence_number: u16,

    /// first sequence number received         
    pub initial_sequence_number: Option<u16>,

    /// number of packets received from this peer,
    /// can differ from max-initial when packets are lost
    pub packets_received: u32,

    /// number of times the sequence number has rolled over from max u16 value          
    pub wrap_around_count: u32,

    /// Stores the arrival time of the WINDOW_SIZE most recent packets
    pub window: VecDeque<u32>,

    /// packet in window with the earliest arrival time
    pub min_window: u32,

    /// middle 32 bytes of the NTP timestamp as received of the last SR from this peer
    pub last_sr_timestamp: Option<u32>,

    /// Time since the last SR has been received
    pub delay_since_last_sr: Option<Instant>,

    /// the expected number of packets received when the last SR was sent
    pub expected_prior: u32,

    /// the received number of packets when the last SR was sent
    pub received_prior: u32,
    // skew_calculator: PeerDelay,

    // buffer where frames with the same timestamp are grouped together
    // playout_buffer: Vec<PlayoutBufferNode>,
}

impl Peer {
    pub fn new(ssrc: u32) -> Self {
        Self {
            ssrc,
            jitter: 0,
            delay_since_last_sr: None,
            last_sr_timestamp: None,
            packets_received: 0,
            wrap_around_count: 0,
            max_sequence_number: 0,
            initial_sequence_number: None,
            window: VecDeque::new(),
            min_window: u32::MAX,
            // playout_buffer: Vec::with_capacity(100),
            // swift_peer_model,
            expected_prior: 0,
            received_prior: 0,
            // skew_calculator: PeerDelay::new(skew_threshold),
        }
    }

    pub fn max_extended_sequence_num(&self) -> u32 {
        let max_sequence = self.max_sequence_number;
        max_sequence as u32 + (65536 * self.wrap_around_count)
    }

    pub fn expected_num_packets(&self) -> u32 {
        match self.initial_sequence_number {
            Some(initial) => {
                self.max_extended_sequence_num()
                    .saturating_sub(initial as u32)
                    + 1
            }
            None => 0,
        }
    }

    pub fn calculate_fraction_lost(&self) -> u8 {
        let expected_interval = self.expected_num_packets() - self.expected_prior;
        let received_inteval = self.packets_received - self.received_prior;
        let lost_inteval = expected_interval as i32 - received_inteval as i32;

        if expected_interval == 0 || lost_inteval <= 0 {
            return 0;
        }

        let fraction = (lost_inteval << 8) / expected_interval as i32;
        fraction.clamp(0, 255) as u8
    }

    pub fn update_reception_stats(&mut self, difference: u32, header: RTPHeader) {
        self.packets_received += 1;

        if let Some(&prev_diff) = self.window.front() {
            let d = difference.wrapping_sub(prev_diff) as i32;
            let d_abs = d.unsigned_abs();
            if d_abs > self.jitter {
                self.jitter += (d_abs - self.jitter) / 16;
            } else {
                self.jitter -= (self.jitter - d_abs) / 16;
            }
        }

        self.window.push_front(difference);

        if self.window.len() > WINDOW_SIZE {
            self.window.pop_back();
        }

        let min = self.window.iter().fold(self.window[0], |min, val| {
            if val.wrapping_sub(min) & 0x80000000 != 0 {
                *val
            } else {
                min
            }
        });

        self.min_window = min;

        if self.initial_sequence_number.is_none() {
            self.initial_sequence_number = Some(header.sequence_number);
            self.max_sequence_number = header.sequence_number;
        }

        let delta = header
            .sequence_number
            .wrapping_sub(self.max_sequence_number);

        if delta < 3000 {
            // accounting for wraparound
            if header.sequence_number < self.max_sequence_number {
                self.wrap_around_count += 1;
            }
            self.max_sequence_number = header.sequence_number;
        } else if delta <= 65535 - 100 {
            // sequence number made a large jump
        } else {
            // misordered packet.
        }
    }

    pub fn update_last_sr_timestamp(&mut self, last_sr_timestamp: u32) {
        self.last_sr_timestamp = Some(last_sr_timestamp);
        self.delay_since_last_sr = Some(Instant::now());
    }

    pub fn reception_report(&mut self) -> ReceptionReport {
        let expected = self.expected_num_packets() as i64;
        let received = self.packets_received as i64;
        let total_lost = (expected - received).clamp(-0x800000, 0x7FFFFF) as i32;

        let fraction_lost = self.calculate_fraction_lost();
        self.expected_prior = self.expected_num_packets();
        self.received_prior = self.packets_received;

        ReceptionReport {
            reportee_ssrc: self.ssrc,
            fraction_lost,
            total_lost,
            extended_sequence_number: self.max_extended_sequence_num(),
            jitter: self.jitter,
            last_sr_timestamp: self.last_sr_timestamp.unwrap_or(0),
            delay_since_last_sr: match self.delay_since_last_sr {
                None => 0,
                Some(time) => {
                    let elapsed = time.elapsed();
                    ((elapsed.as_nanos() * 65536) / 1_000_000_000) as u32
                }
            },
        }
    }
}
