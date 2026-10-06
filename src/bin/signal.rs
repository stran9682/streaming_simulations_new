use std::{collections::HashSet, io, sync::Mutex};

use iroh::{
    Endpoint, EndpointId,
    endpoint::presets,
    protocol::{AcceptError, ProtocolHandler, Router},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    println!("endpoint: {}", endpoint.id());

    let addresses = Addresses::new();

    let _router = Router::builder(endpoint.clone())
        .accept(b"discovery", addresses)
        .spawn();

    tokio::signal::ctrl_c().await?;

    Ok(())
}

#[derive(Debug)]
struct Addresses {
    endpoints: Mutex<HashSet<EndpointId>>,
}

impl ProtocolHandler for Addresses {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let mut buf = [0u8; 32];
            recv.read_exact(&mut buf)
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

            let peer_endpoint = EndpointId::from_bytes(&buf)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

            let endpoints_bytes = {
                let mut endpoints = self
                    .endpoints
                    .lock()
                    .map_err(|_| AcceptError::from_err(io::Error::from(io::ErrorKind::Other)))?;
                let endpoints_copy = endpoints.clone();
                endpoints.insert(peer_endpoint);

                serde_json::to_vec(&endpoints_copy).map_err(AcceptError::from_err)?
            };

            send.write_all(&endpoints_bytes)
                .await
                .map_err(AcceptError::from_err)?;
            send.finish()?;
        }

        Ok(())
    }
}

impl Addresses {
    pub fn new() -> Self {
        Self {
            endpoints: Mutex::new(HashSet::new()),
        }
    }
}
