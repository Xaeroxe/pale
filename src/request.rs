use crate::{Client, PaleError, RPCRequest, Result};

use std::collections::HashMap;

use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde_json::{Value, Error as SerdeJsonError};
use tokio::time::timeout;

impl Client {
    /// Send a request to the specified `method` .  
    ///
    /// For easily inserting params, look at [`params![]`](crate::params).  
    ///
    /// ## Example
    /// ```no_run
    /// let data = client.request::<i32>("method/endpoint", None).await?;
    /// ```
    pub async fn request<T>(
        &self,
        method: impl AsRef<str>,
        params: Option<HashMap<String, Value>>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        if !self.is_connected().await {
            return Err(PaleError::ClientDisconnected);
        }

        let request = RPCRequest::new(method.as_ref().to_string(), params, true);
        let id = request.id.clone();
        self.channels.request.send(request)?;

        let start_time = std::time::Instant::now();
        while let Ok(Ok(res)) = timeout(
            self.config.request_timeout,
            self.channels.response.subscribe().recv(),
        )
        .await
        {
            if res.id != id {
                if start_time.elapsed() > self.config.request_timeout {
                    return Err(PaleError::RequestTimeout);
                }

                continue;
            }

            return if let Some(result) = res.result {
                Ok(serde_json::from_value(result)?)
            } else if let Some(error) = res.error {
                Err(PaleError::RPC(error))
            } else {
                Err(PaleError::NoReturnedRPCData)
            };
        }

        Err(PaleError::RequestTimeoutOrError)
    }

    /// Subscribes to a `method` and returns a stream for any incoming requests from it.  
    ///
    /// The stream gets terminated if the [`Client`] closes.  
    ///
    /// ## Example
    /// ```no_run
    /// let subscription = client.subscribe::<i32>("method:notification/endpoint").await?;
    ///
    /// while let Some(data) = subscription.next().await {
    ///     match data {
    ///         Some(data) => println!("{data}");
    ///         None => ()
    ///     }
    /// }
    /// ```
    pub async fn subscribe<T>(
        &self,
        method: impl AsRef<str>,
    ) -> Result<impl tokio_stream::Stream<Item = Option<std::result::Result<T, SerdeJsonError>>>>
    where
        T: DeserializeOwned,
    {
        if !self.is_connected().await {
            return Err(PaleError::ClientDisconnected);
        }

        let method = method.as_ref().to_string();
        let stream = tokio_stream::wrappers::BroadcastStream::new(self.channels.notis.subscribe());
        let filter = stream
            .filter_map(move |msg| async move {
                msg.ok()
            })
            .take_while(|r| futures::future::ready(!r.is_closing()))
            .filter_map(move |msg| {
                let method = method.clone();
                async move {
                    if msg.method == method {
                        Some(msg.params.map(|v| serde_json::from_value(v)))
                    } else {
                        None
                    }
                }
            });

        Ok(Box::pin(filter))
    }
}
