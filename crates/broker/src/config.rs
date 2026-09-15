//! Broker configuration

use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Address the broker will listen on
    pub listen_addr: SocketAddr,

    /// Broker ID (unique in the cluster)
    pub broker_id: i32,

    /// Data directory for logs and metadata
    pub data_dir: String,

    /// Number of network threads
    pub network_threads: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9092".parse().expect("Invalid default address"),
            broker_id: 1,
            data_dir: "./data".to_string(),
            network_threads: 4,
        }
    }
}

impl BrokerConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(addr) = std::env::var("PULSEMQ_LISTEN_ADDR") {
            if let Ok(parsed) = addr.parse() {
                config.listen_addr = parsed;
            }
        }

        if let Ok(id) = std::env::var("PULSEMQ_BROKER_ID") {
            if let Ok(parsed) = id.parse() {
                config.broker_id = parsed;
            }
        }

        if let Ok(dir) = std::env::var("PULSEMQ_DATA_DIR") {
            config.data_dir = dir;
        }

        config
    }
}
