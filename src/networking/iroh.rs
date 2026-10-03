use std::{
    io::{self},
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::networking::{
    H264_CLOCK_RATE, OPUS_CLOCK_RATE, PacketData, Peer,
    receivers::packet_receiver,
    rtp::RTPSession,
    senders::{send, send_rtcp},
};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{AfterHandshakeOutcome, BeforeConnectOutcome, Connection, EndpointHooks},
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_gossip::ALPN;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;

#[derive(Debug)]
pub struct Iroh {
    bytes_receiver: Receiver<PacketData>,
    clock: Instant,
    session_info: SessionInfo,
}

impl ProtocolHandler for Iroh {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let bytes = recv
            .read_to_end(1000)
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

        let peer_session_info: SessionInfo = serde_json::from_slice(&bytes)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;

        let session_info_bytes = serde_json::to_vec(&self.session_info)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

        send.write_all(&session_info_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()?;

        self.send_rtp(connection, peer_session_info);

        Ok(())
    }
}

impl Iroh {
    pub fn new(
        bytes_receiver: Receiver<PacketData>,
        clock: Instant,
        session_info: SessionInfo,
    ) -> Self {
        Self {
            bytes_receiver,
            clock,
            session_info,
        }
    }

    pub async fn make_request(
        &self,
        endpoint: &Endpoint,
        remote: &EndpointId,
    ) -> anyhow::Result<()> {
        let connection = endpoint.connect(*remote, b"coal").await?;

        let (mut send, mut recv) = connection.open_bi().await?;

        let session_info_bytes = serde_json::to_vec(&self.session_info)?;
        send.write_all(&session_info_bytes).await?;
        send.finish()?;

        let bytes = recv.read_to_end(1000).await?;

        let peer_session_info: SessionInfo = serde_json::from_slice(&bytes)?;

        self.send_rtp(connection, peer_session_info);

        Ok(())
    }

    fn send_rtp(&self, connection: Connection, peer_session_info: SessionInfo) {
        let audio: Arc<RTPSession> = Arc::new(RTPSession::new(
            self.session_info.audio_ssrc,
            OPUS_CLOCK_RATE,
            self.clock,
        ));
        let video: Arc<RTPSession> = Arc::new(RTPSession::new(
            self.session_info.video_ssrc,
            H264_CLOCK_RATE,
            self.clock,
        ));

        let audio_peer = Arc::new(Mutex::new(Peer::new(peer_session_info.audio_ssrc)));
        let video_peer = Arc::new(Mutex::new(Peer::new(peer_session_info.video_ssrc)));

        let rx = self.bytes_receiver.resubscribe();
        let clock = self.clock;

        let send_connection = connection.clone();
        let send_audio_rtp = audio.clone();
        let send_video_rtp = video.clone();
        let send =
            tokio::spawn(
                async move { send(send_connection, send_audio_rtp, send_video_rtp, rx).await },
            );

        let recv_connection = connection.clone();
        let recv_audio_peer = audio_peer.clone();
        let recv_video_peer = video_peer.clone();
        let recv = tokio::spawn(async move {
            packet_receiver(
                recv_connection,
                recv_audio_peer,
                recv_video_peer,
                peer_session_info.video_ssrc,
                clock,
            )
            .await;
        });

        let audio_connection = connection.clone();
        let a_rtcp = tokio::spawn(async move {
            send_rtcp(audio, audio_connection, audio_peer).await;
        });

        let v_rtcp = tokio::spawn(async move {
            send_rtcp(video, connection, video_peer).await;
        });

        tokio::spawn(async move {
            tokio::select! {
                _ = recv => (),
                _ = send => (),
                _ = a_rtcp => (),
                _ = v_rtcp => ()
            }
        });
    }
}

#[derive(Debug)]
pub struct ConnectionTracker {
    active_connections: Mutex<Vec<EndpointId>>,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            active_connections: Mutex::new(Vec::new()),
        }
    }
}

impl EndpointHooks for ConnectionTracker {
    async fn after_handshake<'a>(
        &'a self,
        conn: &'a Connection,
    ) -> iroh::endpoint::AfterHandshakeOutcome {
        if conn.alpn() == ALPN {
            return AfterHandshakeOutcome::Accept;
        }

        let Ok(mut active_connections) = self.active_connections.lock() else {
            return AfterHandshakeOutcome::Reject {
                error_code: 404u32.into(),
                reason: b"Couldn't acquire lock".into(),
            };
        };

        if active_connections.contains(&conn.remote_id()) {
            return AfterHandshakeOutcome::Reject {
                error_code: 403u32.into(),
                reason: b"Already have an active connection with peer".into(),
            };
        }

        active_connections.push(conn.remote_id());

        AfterHandshakeOutcome::Accept
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SessionInfo {
    pub video_ssrc: u32,
    pub audio_ssrc: u32,
}
