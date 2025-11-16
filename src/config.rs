use std::time::Duration;

use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

/// The config used for [`Client`](crate::Client).  
///
/// Controls the timeout [`Duration`]s, max retry attempts, etc.  
///
/// Also contains the [`WebSocketConfig`] for the underlying socket.  
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How many messages can be in any of the internal channels at once.  
    pub channel_capacity: usize,
    /// How many reconnection attempts the client will attempt at most when connection is lost.  
    pub max_retry_attempts: i32,
    /// The delay between each reconnection attempt.  
    pub retry_connection: Duration,
    /// The timeout for each request to the server.  
    pub request_timeout: Duration,
    /// An optional bearer token that will be added into a `Authorization` header on connection.  
    pub bearer_token: Option<String>,
    /// Config for the underlying websocket.  
    pub ws_config: WebSocketConfig,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 8192,
            max_retry_attempts: 36,
            retry_connection: Duration::from_secs(10),
            request_timeout: Duration::from_secs(15),
            bearer_token: None,
            ws_config: WebSocketConfig::default(),
        }
    }
}

impl ClientConfig {
    /// Creates a default [`ClientConfig`] but with a `bearer_token` supplied.  
    pub fn with_bearer(bearer: &str) -> Self {
        Self {
            bearer_token: Some(bearer.to_string()),
            ..Default::default()
        }
    }
}
