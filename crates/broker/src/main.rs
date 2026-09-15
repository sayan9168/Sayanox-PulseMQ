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

    info!("══════════════════════════════════════");
    info!("  Sayanox PulseMQ Broker");
    info!("  Native event streaming platform");
    info!("  Version: {}", env!("CARGO_PKG_VERSION"));
    info!("══════════════════════════════════════");

    let config = BrokerConfig::from_env();
    info!("Broker ID      : {}", config.broker_id);
    info!("Listen address : {}", config.listen_addr);
    info!("Data directory : {}", config.data_dir);

    let storage_config = StorageConfig {
        data_dir: std::path::PathBuf::from(&config.data_dir),
        ..Default::default()
    };

    let storage = match StorageEngine::new(storage_config) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to initialize PulseMQ storage engine: {}", e);
            return;
        }
    };
    info!("PulseMQ Native Storage Engine ready");

    let listener = match TcpListener::bind(config.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind to {}: {}", config.listen_addr, e);
            return;
        }
    };

    info!("PulseMQ is ready — accepting connections");

    let broker_id = config.broker_id;

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!("New connection from {}", addr);
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
                info!("Shutdown signal received");
                break;
            }
        }
    }

    info!("PulseMQ Broker stopped");
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
                    "Request → api={:?} v{} corr={} client={}",
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
                        respond_empty_error(&mut socket, header.correlation_id).await?;
                    }
                }
            }
            None => warn!("Failed to parse request header"),
        }
    }

    Ok(())
}

async fn respond_api_versions(
    socket: &mut TcpStream,
    correlation_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response = BytesMut::new();
    response.put_i32(0);

    response.put_i32(correlation_id);
    response.put_i16(0);

    let apis = [
        (0i16, 0i16, 9i16),
        (1i16, 0i16, 11i16),
        (3i16, 0i16, 12i16),
        (18i16, 0i16, 3i16),
    ];

    response.put_i32(apis.len() as i32);
    for (key, min_v, max_v) in apis {
        response.put_i16(key);
        response.put_i16(min_v);
        response.put_i16(max_v);
    }

    response.put_i32(0);

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    Ok(())
}

async fn respond_metadata(
    socket: &mut TcpStream,
    header: &RequestHeader,
    broker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response = BytesMut::new();
    response.put_i32(0);

    response.put_i32(header.correlation_id);
    response.put_i32(0);

    response.put_i32(1);
    response.put_i32(broker_id);
    let host = "localhost";
    response.put_i16(host.len() as i16);
    response.put_slice(host.as_bytes());
    response.put_i32(9092);
    response.put_i16(-1);

    let cluster_id = "pulsemq-native";
    response.put_i16(cluster_id.len() as i16);
    response.put_slice(cluster_id.as_bytes());

    response.put_i32(broker_id);
    response.put_i32(0); // no topics yet

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    info!("Metadata response sent");
    Ok(())
}

async fn respond_produce(
    socket: &mut TcpStream,
    header: &RequestHeader,
    storage: &StorageEngine,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Compatibility path: we still accept Kafka Produce requests,
    // but we store data in PulseMQ native format.
    let stream = "test-stream";
    let partition = 0;

    let record = Record {
        key: None,
        value: b"hello from PulseMQ native storage".to_vec(),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        headers: vec![("source".into(), b"pulsemq-compat".to_vec())],
    };

    match storage.append(stream, partition, &[record]) {
        Ok(offsets) => {
            info!("Stored record in native format → {}-{} offsets={:?}", stream, partition, offsets);

            let mut response = BytesMut::new();
            response.put_i32(0);
            response.put_i32(header.correlation_id);
            response.put_i32(0);

            response.put_i32(1);
            response.put_i16(stream.len() as i16);
            response.put_slice(stream.as_bytes());

            response.put_i32(1);
            response.put_i32(partition);
            response.put_i16(0);
            response.put_i64(offsets[0]);
            response.put_i64(-1);
            response.put_i64(-1);

            let size = (response.len() - 4) as i32;
            response[0..4].copy_from_slice(&size.to_be_bytes());

            socket.write_all(&response).await?;
            socket.flush().await?;
        }
        Err(e) => {
            error!("Native storage append failed: {}", e);
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
    response.put_i16(1);

    let size = (response.len() - 4) as i32;
    response[0..4].copy_from_slice(&size.to_be_bytes());

    socket.write_all(&response).await?;
    socket.flush().await?;
    Ok(())
}
