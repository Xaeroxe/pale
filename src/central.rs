use futures::{FutureExt, SinkExt, StreamExt};
use serde_json::Value;
use tokio::{select, task::JoinHandle};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

use crate::{Client, NotisResponse, RPCResponse, Result};

impl Client {
    /// Starts the main websocket reconnection, listening and channel communication for requests and subcriptions.  
    pub(crate) async fn run(self) {
        let mut retry_attempts = -1;
        let mut close_connection = false;
        loop {
            if close_connection {
                {
                    *self.client_connected.lock().await = false;
                }
                tracing::debug!("Closed connection");

                if let Err(e) = self.close_all_subscriptions().await {
                    tracing::error!(?e);
                }

                break;
            }

            let ws = match Self::connect_websocket(self.uri.clone(), &self.config).await {
                Ok(ws) => {
                    {
                        *self.client_connected.lock().await = true;
                    }

                    // first connection is -1
                    if retry_attempts != -1 {
                        self.channels.on_reconnect.send(self.clone()).unwrap();
                    }

                    retry_attempts = 0;

                    ws
                }
                Err(e) => {
                    if retry_attempts > self.config.max_retry_attempts {
                        tracing::error!("failed to reconnect after max_retry_attempts");
                        break;
                    }

                    tracing::debug!("failed to reconnect, {e:?}");
                    {
                        *self.client_connected.lock().await = false;
                    }
                    tokio::time::sleep(self.config.retry_connection).await;
                    retry_attempts += 1;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // response
            let _self = self.clone();
            let handle_res: JoinHandle<Result<()>> = tokio::spawn(async move {
                while let Some(msg) = read.next().await {
                    if let Ok(msg) = msg {
                        let response: Value = match msg {
                            Message::Text(data) => serde_json::from_str(data.as_str())?,
                            Message::Binary(data) => serde_json::from_slice(&data)?,
                            Message::Close(_) => return Ok(()),
                            _ => continue,
                        };

                        tracing::trace!(?response);

                        // if we got params, its a notis
                        if response.get("params").is_some() {
                            let data: NotisResponse = serde_json::from_value(response)?;
                            if _self.channels.notis.receiver_count() > 0 {
                                _self.channels.notis.send(data)?;
                            }
                        // otherwise normal request response
                        } else {
                            let data: RPCResponse = serde_json::from_value(response)?;
                            if _self.channels.response.receiver_count() > 0 {
                                _self.channels.response.send(data)?;
                            } else {
                                tracing::debug!("Ignored res send, no receivers");
                            }
                        }
                    }
                }

                // if we are here, we lost connection
                if let Err(e) = _self.channels.on_disconnect.send(_self.clone()) {
                    tracing::debug!(?e);
                };

                Ok(())
            });

            // request
            let mut _req = self.channels.request.subscribe();
            let handle_req: JoinHandle<Result<()>> = tokio::spawn(async move {
                while let Ok(req) = _req.recv().await {
                    tracing::debug!("sending {req:?}");
                    write
                        .send(Message::Text(Utf8Bytes::from(serde_json::to_string(&req)?)))
                        .await?;
                }

                Ok(())
            });

            // needed so we can on demand close down this entire function.
            let _close = self.channels.close.1.clone();
            let close_h = tokio::spawn(async move {
                while let Some(_) = _close.lock().await.recv().await {
                    return true;
                }
                false
            });

            select! {
                result = handle_res.fuse() => {
                    if let Ok(Err(e)) = result {
                        tracing::error!(?e);
                    }
                },
                result = handle_req.fuse() => {
                    if let Ok(Err(e)) = result {
                        tracing::error!(?e);
                    }
                },
                close_ws = close_h.fuse() => {
                    close_connection = close_ws.unwrap_or(false);
                    continue;
                }
            };

            tracing::debug!(
                "either req or res handles were finished, probably lost websocket connection"
            );
        }
    }
}
