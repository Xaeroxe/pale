use thiserror::Error;
use tokio_tungstenite::tungstenite::{self};

use crate::{NotisResponse, RPCError, RPCRequest, RPCResponse};

pub type Result<T> = std::result::Result<T, PaleError>;

#[derive(Debug, Error)]
pub enum PaleError {
    #[error("{0:?}")]
    RPC(RPCError),
    #[error("Neither result or error was populated")]
    NoReturnedRPCData,
    #[error("Request timeout")]
    RequestTimeout,
    #[error("The request either timeout or got None")]
    RequestTimeoutOrError,
    #[error("The underlying connection was either dropped or took too long to reconnect")]
    ClientDisconnected,
    #[error("The client is already connected")]
    ClientAlreadyConnected,
    #[error("Client timeout on connection: {0:?}")]
    ClientTimeout(#[from] tokio::time::error::Elapsed),
    #[error(transparent)]
    Tungstenite(#[from] tungstenite::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    InvalidUri(#[from] tokio_tungstenite::tungstenite::http::uri::InvalidUri),
    #[error(transparent)]
    RequestSendError(#[from] tokio::sync::broadcast::error::SendError<RPCRequest>),
    #[error(transparent)]
    CloseSendError(#[from] tokio::sync::mpsc::error::SendError<()>),
    #[error(transparent)]
    ResponseSendError(#[from] tokio::sync::broadcast::error::SendError<RPCResponse>),
    #[error(transparent)]
    ValueSendError(#[from] tokio::sync::broadcast::error::SendError<NotisResponse>),
}
