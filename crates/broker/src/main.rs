mod config;

use bytes::{BufMut, BytesMut};
use config::BrokerConfig;
use pulsemq_protocol::{parse_request_header, parse_request_size, ApiKey, RequestHeader};
use pulsemq_storage::{Record, StorageConfig, StorageEngine};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

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

    // Initialize Storage Engine
    let storage_config = StorageConfig {
        data_dir: std::path::PathBuf::from(&config.data_dir),
        ..Default::default()
    };

    let storage = match StorageEngine::new(storage_config) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to initialize storage engine: {}", e);
            return;
        }
    };
    info!("Storage engine initialized");

    let listener = match TcpListener::bind(config.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", config.listen_addr, e);
            return;
        }
    };

    info!("PulseMQ Broker is ready and accepting connections");

    let broker_id = config.broker_id;

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!("New connection from: {}", addr);
                        let storage = Arc::clone(&storage);
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(socket, storage, broker_id).await {
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

async fn handle_connection(
    mut socket: TcpStream,
    storage: Arc<StorageEngine>,
    broker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let n = socket.read_buf(&mut buffer).await?;
        if n == 0 {
            debug!("Client closed connection");
            break;
        }

        let size = match parse_request_size(&mut buffer)? {
            Some(s) => s,
            None => continue,
        };

        while buffer.len() < size as usize {
            let n = socket.read_buf(&mut buffer).await?;
            if n == 0 {
                return Ok(());
            }
        }

        match parse_request_header(&mut buffer)? {
            Some(header) => {
                info!(
                    "Received request: api_key={:?}, version={}, correlation_id={}, client_id={}",
                    header.api_key, header.api_version, header.correlation_id, header.client_id
                );

                match header.api_key {
                    ApiKey::ApiVersions => {
                        respond_api_versions(&mut socket, header.correlation_id).await?;
                    }
                    ApiKey::Metadata => {
                        respond_metadata(&mut socket, &header, broker_id).await?;
                    }
                    ApiKey::Produce => {
                        respond_produce(&mut socket, &header, &storage).await?;
                    }
                    other => {
                        warn!("Unhandled API key: {:?}", other);
                        // Send a generic error response so client doesn't hang
                        respond_empty_error(&mut socket, header.correlation_id).await?;
                    }
                }
            }
            None => {
                warn!("Failed to parse request header");
            }
        }
    }

    Ok(())
}

async fn respond_api_versions(
    socket: &mut TcpStream,
    correlation_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response = BytesMut::new();
    response.put_i32(0); // size placeholder

    response.put_i32(correlation_id);
    response.put_i16(0); // error_code = NONE

    // api_versions array (compact style simplified)
    // We advertise a few basic APIs
    let apis = [
        (0i16, 0i16, 9i16),   // Produce
        (1i16, 0i16, 11i16),  // Fetch
        (3i16, 0i16, 12i16),  // Metadata
        (18i16, 0i16, 3i16),  // ApiVersions
    ];

    response.put_i32(apis.len() as i32);
    for (key, min_v, max_v) in apis {
        response.put_i16(key);
        response.put_i16(min_v);
        response.put_i16(max_v);
    }

    response.put_i32(0); // throttle_time_ms

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    info!("Sent ApiVersions response");
    Ok(())
}

async fn respond_metadata(
    socket: &mut TcpStream,
    header: &RequestHeader,
    broker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response = BytesMut::new();
    response.put_i32(0); // size placeholder

    response.put_i32(header.correlation_id);
    response.put_i32(0); // throttle_time_ms

    // Brokers array
    response.put_i32(1); // one broker
    response.put_i32(broker_id);
    // host (string)
    let host = "localhost";
    response.put_i16(host.len() as i16);
    response.put_slice(host.as_bytes());
    response.put_i32(9092); // port
    // rack (nullable string) = null
    response.put_i16(-1);

    // cluster_id (nullable string)
    let cluster_id = "pulsemq-cluster";
    response.put_i16(cluster_id.len() as i16);
    response.put_slice(cluster_id.as_bytes());

    response.put_i32(broker_id); // controller_id

    // Topics array - for now return empty (no topics yet)
    response.put_i32(0);

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    info!("Sent Metadata response (broker_id={})", broker_id);
    Ok(())
}

async fn respond_produce(
    socket: &mut TcpStream,
    header: &RequestHeader,
    storage: &StorageEngine,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // For now we do a simplified produce:
    // We don't fully parse the produce request body yet.
    // Instead we create a dummy record and append it to a default topic.
    // Full Produce request parsing will be added next.

    let topic = "test-topic";
    let partition = 0;

    let record = Record {
        key: None,
        value: b"hello from PulseMQ produce".to_vec(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    };

    match storage.append(topic, partition, &[record]) {
        Ok(offsets) => {
            info!("Produced record to {}-{} at offsets={:?}", topic, partition, offsets);

            // Send a basic successful produce response
            let mut response = BytesMut::new();
            response.put_i32(0); // size placeholder
            response.put_i32(header.correlation_id);
            response.put_i32(0); // throttle_time_ms

            // responses array (one topic)
            response.put_i32(1);
            // topic name
            response.put_i16(topic.len() as i16);
            response.put_slice(topic.as_bytes());

            // partition responses
            response.put_i32(1);
            response.put_i32(partition);
            response.put_i16(0); // error_code = NONE
            response.put_i64(offsets[0]); // base_offset
            response.put_i64(-1); // log_append_time
            response.put_i64(-1); // log_start_offset

            let size = (response.len() - 4) as i32;
            response[0..4].copy_from_slice(&size.to_be_bytes());

            socket.write_all(&response).await?;
            socket.flush().await?;
            info!("Sent Produce response");
        }
        Err(e) => {
            error!("Failed to append record: {}", e);
            respond_empty_error(socket, header.correlation_id).await?;
        }
    }

    Ok(())
}

async fn respond_empty_error(
    socket: &mut TcpStream,
    correlation_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response = BytesMut::new();
    response.put_i32(0);
    response.put_i32(correlation_id);
    response.put_i16(1); // non-zero error

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    Ok(())
}
