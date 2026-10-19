use std::time::Instant;

use crate::networking::rtcp::ReceptionReport;

#[derive(Debug)]
pub struct Peer {
    pub ssrc: u32,

    /// variance in arrival time
    pub jitter: f64,

    /// highest sequence number currently received from this peer         
    pub max_sequence_number: u16,

    /// first sequence number received         
    pub initial_sequence_number: Option<u16>,

    /// number of packets received from this peer,
    /// can differ from max-initial when packets are lost
    pub packets_received: u32,

    /// number of times the sequence number has rolled over from max u16 value          
    pub wrap_around_count: u32,

    /// Transit difference of the previous packet for RFC 3550 jitter calculation
    pub prev_difference: Option<u32>,

    /// middle 32 bytes of the NTP timestamp as received of the last SR from this peer
    pub last_sr_timestamp: Option<u32>,

    /// Time since the last SR has been received
    pub delay_since_last_sr: Option<Instant>,

    /// the expected number of packets received when the last SR was sent
    pub expected_prior: u32,

    /// the received number of packets when the last SR was sent
    pub received_prior: u32,
}

impl Peer {
    pub fn new(ssrc: u32) -> Self {
        Self {
            ssrc,
            jitter: 0.0,
            delay_since_last_sr: None,
            last_sr_timestamp: None,
            packets_received: 0,
            wrap_around_count: 0,
            max_sequence_number: 0,
            initial_sequence_number: None,
            prev_difference: None,
            expected_prior: 0,
            received_prior: 0,
        }
    }

    #[inline]
    pub fn max_extended_sequence_num(&self) -> u32 {
        let max_sequence = self.max_sequence_number;
        max_sequence as u32 + (65536 * self.wrap_around_count)
    }

    #[inline]
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

    #[inline]
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

    #[inline]
    pub fn update_reception_stats(&mut self, difference: u32, sequence_number: u16) {
        self.packets_received += 1;

        if let Some(prev_diff) = self.prev_difference {
            let d = difference.wrapping_sub(prev_diff) as i32;
            let d_abs = d.unsigned_abs();
            self.jitter += (1.0 / 16.0) * (d_abs as f64 - self.jitter);
        }
        self.prev_difference = Some(difference);

        if self.initial_sequence_number.is_none() {
            self.initial_sequence_number = Some(sequence_number);
            self.max_sequence_number = sequence_number;
        }

        let delta = sequence_number.wrapping_sub(self.max_sequence_number);

        if delta < 3000 {
            // accounting for wraparound
            if sequence_number < self.max_sequence_number {
                self.wrap_around_count += 1;
            }
            self.max_sequence_number = sequence_number;
        } else if delta <= 65535 - 100 {
            // sequence number made a large jump
        } else {
            // misordered packet.
        }
    }

    #[inline]
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
            jitter: self.jitter as u32,
            last_sr_timestamp: self.last_sr_timestamp.unwrap_or(0),
            delay_since_last_sr: match self.delay_since_last_sr {
                None => 0,
                Some(time) => {
                    let elapsed = time.elapsed();
                    ((elapsed.as_micros() as u64 * 65536) / 1_000_000) as u32
                }
            },
        }
    }
}
