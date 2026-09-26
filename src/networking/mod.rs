use bytes::Bytes;

pub mod iroh;
pub mod peer;
pub mod receivers;
pub mod rtcp;
pub mod rtp;
pub mod senders;

pub use peer::Peer;

#[derive(Clone)]
pub enum PacketType {
    Video,
    Audio,
}

#[derive(Clone)]
pub struct PacketData {
    pub packet_type: PacketType,
    pub data: Bytes,
    pub timestamp: u32,
}

const OPUS_CLOCK_RATE: u64 = 48_000;
const H264_CLOCK_RATE: u64 = 90_000;
