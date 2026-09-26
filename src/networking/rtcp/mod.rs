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
