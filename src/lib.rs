#![doc = include_str!("../readme.md")]

// TODO: Currently all doc tests aren't being ran and theres no real tests.
// this is due to us needing a mock json rpc 2.0 *server* that all tests can send to
// and use for testing purposes that can be ran easily locally.

mod central;
mod client;
mod config;
mod error;
mod macros;
mod request;
mod types;

pub use client::Client;
pub use config::ClientConfig;
pub use error::*;
pub use types::*;

// Re-export StreamExt because it's needed in subscription streams
pub use futures_util::StreamExt;
// and WebSocketConfig since its use in the ClientConfig
pub use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
