mod config;

use config::BrokerConfig;
use tokio::net::TcpListener;
use tracing::{info, error, warn};

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    info!("Sayanox PulseMQ Broker starting...");
    info!("Version: {}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = BrokerConfig::from_env();
    info!("Broker ID: {}", config.broker_id);
    info!("Listening on: {}", config.listen_addr);
    info!("Data directory: {}", config.data_dir);

    // TODO: Initialize storage engine
    // TODO: Start background tasks (replication, compaction, etc.)

    // Start TCP listener (foundation for Kafka protocol)
    let listener = match TcpListener::bind(config.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", config.listen_addr, e);
            return;
        }
    };

    info!("PulseMQ Broker is ready and accepting connections");

    // Accept connections loop
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!("New connection from: {}", addr);
                        // TODO: Handle Kafka protocol handshake & requests
                        tokio::spawn(async move {
                            handle_connection(socket).await;
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
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

async fn handle_connection(socket: tokio::net::TcpStream) {
    // Placeholder for future Kafka protocol handling
    warn!("Connection handler not yet implemented (Kafka protocol coming soon)");
    // For now just drop the connection
    drop(socket);
}
