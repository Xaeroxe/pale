# pale

> A Reconnecting, subscription keeping, clonable JSON RPC 2.0 Websocket Client in Rust

> [!WARNING]  
> Use on your own risk, this was just made to suit a very specific use case that i personally needed.  
> I have not benchmarked this or checked any performance. And isn't thoroughly tested and probably have issues.  
> But hopefully the code isn't too much to scan over if you encounter any issues <3


## Example

```rust
// create & connect a client
let client = Client::new("ws://example.com", ClientConfig::default()).await?;

// a simple request
let data = client.request::<Vec<i32>>("example/method", params![("param_name", true)]).await?;

// a subscription stream
let mut sub = client.subscribe::<String>("example/notification").await?;
while let Some(data) = sub.next().await {
    println!("{data:?}");
}
```