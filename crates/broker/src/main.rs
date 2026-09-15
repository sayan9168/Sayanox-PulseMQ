mod config;

use bytes::BytesMut;
use config::BrokerConfig;
use pulsemq_protocol::{parse_request_header, parse_request_size, ApiKey};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn, debug};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Sayanox PulseMQ Broker starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    let config = BrokerConfig::from_env();
    info!("Broker ID: {}", config.broker_id);
    info!("Listening on: {}", config.listen_addr);
    info!("Data directory: {}", config.data_dir);

    // TODO: Initialize storage engine

    let listener = match TcpListener::bind(config.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", config.listen_addr, e);
            return;
        }
    };

    info!("PulseMQ Broker is ready and accepting connections");

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!("New connection from: {}", addr);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(socket).await {
                                warn!("Connection error from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept connection: {}", e),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
                break;
            }
        }
    }

    info!("Shutting down PulseMQ Broker...");
}

async fn handle_connection(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read more data
        let n = socket.read_buf(&mut buffer).await?;
        if n == 0 {
            debug!("Client closed connection");
            break;
        }

        // Try to parse request size
        let size = match parse_request_size(&mut buffer)? {
            Some(s) => s,
            None => continue, // Need more data
        };

        // Ensure we have the full request body
        while buffer.len() < size as usize {
            let n = socket.read_buf(&mut buffer).await?;
            if n == 0 {
                return Ok(()); // Client disconnected mid-request
            }
        }

        // Parse the request header
        match parse_request_header(&mut buffer)? {
            Some(header) => {
                info!(
                    "Received request: api_key={:?}, version={}, correlation_id={}, client_id={}",
                    header.api_key, header.api_version, header.correlation_id, header.client_id
                );

                // Handle known API keys (stub responses for now)
                match header.api_key {
                    ApiKey::ApiVersions => {
                        // Very basic ApiVersions response (placeholder)
                        respond_api_versions(&mut socket, header.correlation_id).await?;
                    }
                    ApiKey::Metadata => {
                        info!("Metadata request received (handler not fully implemented yet)");
                    }
                    other => {
                        warn!("Unhandled API key: {:?}", other);
                    }
                }
            }
            None => {
                // Should not happen if size was correct, but just in case
                warn!("Failed to parse request header after receiving full size");
            }
        }
    }

    Ok(())
}

/// Minimal ApiVersions response so Kafka clients can at least connect
async fn respond_api_versions(
    socket: &mut TcpStream,
    correlation_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // This is a very simplified response just to acknowledge the request.
    // Full implementation will come later.
    let mut response = BytesMut::new();

    // Response size placeholder (will fill later)
    response.extend_from_slice(&[0, 0, 0, 0]);

    // Correlation ID
    response.extend_from_slice(&correlation_id.to_be_bytes());

    // Error code = 0
    response.extend_from_slice(&0i16.to_be_bytes());

    // For now we send a minimal empty-ish response so the client doesn't hang.
    // Proper ApiVersions response structure will be implemented next.

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;

    info!("Sent basic ApiVersions response (correlation_id={})", correlation_id);
    Ok(())
}
