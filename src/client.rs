use std::{str::FromStr, sync::Arc, time::Duration};

use tokio::{
    net::TcpStream,
    sync::{
        Mutex,
        broadcast::{Receiver, Sender, channel},
        mpsc,
    },
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{ClientRequestBuilder, http::Uri},
};

use crate::{ClientConfig, NotisResponse, PaleError, RPCRequest, RPCResponse, Result};

/// A JSON RPC 2.0 Websocket Client.  
///
/// Use [`request`](Self::request) and [`subscribe`](Self::subscribe) to communicate via the connection.  
///
/// Client can also be controlled more via [`close`](Self::close), [`connect`](Self::connect) and [`is_connected`](Self::is_connected).  
///
/// ## Reconnection
/// This client handles reconnecting when the socket is lost.  
/// Max retry attempts and timeout durations can be customized via [`ClientConfig`].  
/// Subscriptions remain after reconnections.  
#[derive(Debug, Clone)]
pub struct Client {
    /// All `tokio::sync::*` related channels to the inner communication of the client.  
    pub(crate) channels: Channels,
    /// If the client is actively connected or not.  
    pub(crate) client_connected: Arc<Mutex<bool>>,
    /// The supplied uri that the underlying socket connects to.  
    pub uri: Arc<String>,
    /// The config for the [`Client`] and the underlying [`WebSocket`](WebSocketStream).  
    pub config: Arc<ClientConfig>,
}

/// All channel related `Sender`s and `Receiver`s.  
///
/// Used for all core communication within a [`Client`] from requests to responses,
/// RPC Notifications and state related to the underlying socket itself.  
#[derive(Debug, Clone)]
pub(crate) struct Channels {
    /// Used for broadcasting any request-responses from the server.  
    pub response: Arc<Sender<RPCResponse>>,
    /// Broadcasts all notifications from the server.  
    pub notis: Arc<Sender<NotisResponse>>,
    /// Used for sending requests to the server from any request call.  
    pub request: Arc<Sender<RPCRequest>>,
    /// Is emitted when the client successfully reconnects.  
    ///
    /// Notably doesn't run on the first successful connection.  
    pub on_reconnect: Arc<Sender<Client>>,
    /// Is emitted when the underlying socket is disconnected.  
    pub on_disconnect: Arc<Sender<Client>>,
    /// Emits when the [`Client`] is constructed to close.  
    ///
    /// And can be requested to close down the [`Client`].  
    pub close: (Arc<mpsc::Sender<()>>, Arc<Mutex<mpsc::Receiver<()>>>),
}

impl Channels {
    /// Initializes all the channels with the specified `capacity` (except `close` which has a capacity of 1).  
    pub(crate) fn new(capacity: usize) -> Self {
        let close = mpsc::channel(1);
        Self {
            response: Arc::new(channel::<RPCResponse>(capacity).0),
            notis: Arc::new(channel::<NotisResponse>(capacity).0),
            on_reconnect: Arc::new(channel::<Client>(capacity).0),
            on_disconnect: Arc::new(channel::<Client>(capacity).0),
            request: Arc::new(channel::<RPCRequest>(capacity).0),
            close: (Arc::new(close.0), Arc::new(Mutex::new(close.1))),
        }
    }
}

impl Client {
    /// The JSON RPC version
    pub(crate) const JSONRPC: &'static str = "2.0";

    /// Creates a new [`Client`] and connects to the supplied `uri`.  
    ///
    /// ## Example
    /// ```no_run
    /// // Creates a new client
    /// let client = Client::new("ws://example.com", ClientConfig::default()).await?;
    /// // Request example
    /// let data = client.request::<Vec<String>>("method/endpoint", HashMap::new()).await?;
    /// ```
    pub async fn new(uri: impl AsRef<str>, config: ClientConfig) -> Result<Self> {
        let client = Self::new_without_connection(uri, config)?;
        client.connect().await?;

        Ok(client)
    }

    /// Creates a new [`Client`] without connecting to the socket.  
    ///
    /// This initializes the [`Client`] and creates all the internal channels.  
    ///
    /// ## Example
    /// ```no_run
    /// Client::new_without_connection("ws://example.com", ClientConfig::default()).await?;
    /// ```
    pub fn new_without_connection(uri: impl AsRef<str>, config: ClientConfig) -> Result<Self> {
        let client = Self {
            channels: Channels::new(config.channel_capacity),
            client_connected: Arc::new(Mutex::new(false)),
            uri: Arc::new(uri.as_ref().to_string()),
            config: Arc::new(config),
        };

        Ok(client)
    }

    /// Connects to the underlying websocket with the provided configuration.  
    pub(crate) async fn connect_websocket(
        uri: Arc<String>,
        config: &ClientConfig,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut req = ClientRequestBuilder::new(Uri::from_str(uri.as_ref())?);
        if let Some(bearer) = &config.bearer_token {
            req = req.with_header("Authorization", format!("Bearer {bearer}"));
            tracing::debug!("Added 'Authorization' to websocket connection");
        }

        let (ws, _) = connect_async_with_config(req, Some(config.ws_config), false).await?;
        tracing::debug!(
            "Connected to server '{}' ({:?})",
            uri.as_ref(),
            config.ws_config
        );

        Ok(ws)
    }

    /// Sends a special [`NotisResponse`] that signals to all subscriptions to end their stream.  
    pub(crate) async fn close_all_subscriptions(&self) -> Result<()> {
        self.channels.notis.send(NotisResponse {
            jsonrpc: Client::JSONRPC.to_string(),
            method: String::default(),
            params: None,
            _close: Some(true),
        })?;
        tracing::debug!("Closed all subscriptions");

        Ok(())
    }

    /// Calling [`Self::close`] means:
    /// - Closing the underlying connection.  
    /// - Any and all internal client communication
    /// - Closing every subscription stream
    ///
    /// The [`Client`] is not garuanted to be 100% closed after this function returns.  
    /// It may take a little while, use [`Self::wait_for_connection`] to make sure before, let's say reconnecting.  
    pub async fn close(&self) -> Result<()> {
        Ok(self.channels.close.0.send(()).await?)
    }

    /// Returns if the underlying socket is actively connected.  
    pub async fn is_connected(&self) -> bool {
        *self.client_connected.lock().await
    }

    /// Returns when the [`Self::is_connected`] is equal to `state`
    ///
    /// If [`Self::is_connected`] already matches `state`, it instantly returns
    ///
    /// ## Example
    /// ```no_run
    /// // waits for the underlying connection to be ready & connected
    /// client.wait_for_connection(true).await;
    ///
    /// // waits for the connection to disconnect
    /// client.wait_for_connection(false).await;
    /// ```
    pub async fn wait_for_connection(&self, state: bool, timeout_duration: Duration) -> Result<()> {
        if self.is_connected().await == state {
            return Ok(());
        }

        let _client = self.clone();
        let client_check = async {
            loop {
                if _client.is_connected().await == state {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        timeout(timeout_duration, client_check).await?;

        Ok(())
    }

    /// Tries to connect to the specified [`Uri`] with the [`ClientConfig`].  
    ///
    /// This spawns an underlying background task for processing everything in the [`Client`].  
    ///
    /// ----
    ///
    /// Will return [`PaleError::ClientAlreadyConnected`] if the client already has an underlying connection and is processing communication.  
    ///
    /// Call [`Self::close`] and check [`Self::is_connected`] or [`Self::wait_for_connection`] if you would want to reconnect entirely.  
    ///
    /// ## Example
    /// ```no_run
    /// // the client is normally connected upon creation so we close it first.  
    /// client.close().await?;
    /// client.wait_for_connection(false).await?;
    ///
    /// client.connect().await?;
    /// ```
    pub async fn connect(&self) -> Result<()> {
        if self.is_connected().await {
            return Err(PaleError::ClientAlreadyConnected);
        }

        let _client = self.clone();
        tokio::spawn(async move { _client.run().await });

        self.wait_for_connection(true, self.config.retry_connection)
            .await?;

        Ok(())
    }

    /// Returns a [`Receiver`] where a message of type [`Client`] will be sent upon each successful reconnection.  
    pub fn on_reconnect(&self) -> Receiver<Client> {
        self.channels.on_reconnect.subscribe()
    }

    /// Returns a [`Receiver`] where a message of type [`Client`] will be sent upon disconnect.  
    pub fn on_disconnect(&self) -> Receiver<Client> {
        self.channels.on_disconnect.subscribe()
    }
}
