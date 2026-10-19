use bytes::{Buf, BufMut, BytesMut};

use crate::networking::rtcp::PacketType;

/*
     0                   1                   2                   3
     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    |V=2|P|    RC   |   PT=SR=200   |             length            |
    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/
pub struct RTCPHeader {
    pub padding: bool,
    pub count: u8,
    pub packet_type: PacketType,
    pub length: u16,
}

impl RTCPHeader {
    #[inline]
    pub fn serialize_into(&self, buf: &mut BytesMut) {
        let b0 = (2 << 6) | ((self.padding as u8) << 5) | (self.count & 0x1f);

        buf.put_u8(b0);
        buf.put_u8(self.packet_type as u8);
        buf.put_u16(self.length);
    }

    pub fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(4);
        self.serialize_into(&mut buf);
        buf
    }

    pub fn deserialize<B: Buf>(packet: &mut B) -> RTCPHeader {
        let b0 = packet.get_u8();
        //let version = (b0 >> VERSION_SHIFT) & VERSION_MASK;

        let padding = ((b0 >> 5) & 0x1) > 0;
        let count = (b0 >> 0) & 0x1f;
        let packet_type = PacketType::from(packet.get_u8());
        let length = packet.get_u16();

        RTCPHeader {
            padding,
            count,
            packet_type,
            length,
        }
    }
}
