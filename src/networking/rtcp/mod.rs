use std::time::{Duration, SystemTime};

pub mod reception_report;
pub mod rtcp_header;
pub mod sender_report;

pub use reception_report::ReceptionReport;
pub use rtcp_header::RTCPHeader;
pub use sender_report::SenderReport;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PacketType {
    Unsupported = 0,
    SenderReport = 200,      // RFC 3550, 6.4.1
    SourceDescription = 202, // RFC 3550, 6.5
    Goodbye = 203,           // RFC 3550, 6.6
}

impl PacketType {
    fn from(b: u8) -> Self {
        match b {
            200 => PacketType::SenderReport,      // RFC 3550, 6.4.1
            202 => PacketType::SourceDescription, // RFC 3550, 6.5
            203 => PacketType::Goodbye,           // RFC 3550, 6.6
            _ => PacketType::Unsupported,
        }
    }
}

pub fn system_time_to_ntp(now: SystemTime) -> u64 {
    let time_since_epoch = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();

    let seconds = time_since_epoch.as_secs() + 2_208_988_800;
    let fraction =
        ((time_since_epoch.subsec_micros() + 1) as f64 * (1u64 << 32) as f64 * 1.0e-6) as u32;
    seconds << 32 | (fraction as u64)
}

pub fn ntp_to_middle_32(ntp: u64) -> u32 {
    ((ntp >> 16) & 0xFFFFFFFF) as u32
}

pub fn calculate_rtt(last_sr: u32, dlsr: u32, arrival_ntp_middle_32: u32) -> Option<Duration> {
    if last_sr == 0 {
        return None;
    }

    let elapsed = arrival_ntp_middle_32.wrapping_sub(last_sr);

    if elapsed >= 0x8000_0000 {
        return None;
    }

    let rtt_units = elapsed.saturating_sub(dlsr);
    let rtt_nanos = (rtt_units as u128 * 1_000_000_000) / 65536;
    Some(Duration::from_nanos(rtt_nanos as u64))
}
