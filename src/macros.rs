/// Can be used to easily insert parameters into [`Client::request`] functions.  
///
/// Does trigger a panic if it fails to serialize the data into json.  
/// May also have to install `serde_json = "1"` in your cargo.toml, idk if it works cross crate boundary.  
///
/// ## Example
/// ```no_run
/// let client = Client::new("ws://localhost:5281", ClientConfig::default()).await?;
/// let data = client.request::<usize>("example/method", params![("filter", 81)]).await?;
/// ```
#[macro_export]
macro_rules! params {
    ( $( ($key:expr, $value:expr) ),* $(,)? ) => {{
        let mut map: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
        $(
            map.insert(
                $key.to_string(),
                serde_json::to_value(&$value).expect("Failed to convert value to json"),
            );
        )*
        Some(map)
    }};
}
