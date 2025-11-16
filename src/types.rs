use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::Client;

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct RPCRequest {
    pub(crate) jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct NotisResponse {
    #[allow(unused)]
    pub(crate) jsonrpc: String,
    pub method: String,
    pub params: Option<Value>,
    /// Due to the `skip_deserializing` attribute on this field.  
    /// It cant be set by outside connections and only internals of the [`Client`] can even touch it.  
    #[serde(skip_deserializing)]
    pub(crate) _close: Option<bool>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct RPCResponse {
    #[allow(unused)]
    pub(crate) jsonrpc: String,
    pub result: Option<Value>,
    pub error: Option<RPCError>,
    pub id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct RPCError {
    pub code: i32,
    pub message: String,
    pub data: Option<Value>,
}

impl NotisResponse {
    pub(crate) fn is_closing(&self) -> bool {
        tracing::debug!("_close: {:?}", self._close);
        self._close.unwrap_or(false)
    }
}

impl RPCRequest {
    pub fn new(method: String, params: Option<HashMap<String, Value>>, generate_id: bool) -> Self {
        Self {
            jsonrpc: Client::JSONRPC.to_string(),
            method,
            params,
            id: if generate_id {
                Some(Uuid::new_v4())
            } else {
                None
            },
        }
    }
}
