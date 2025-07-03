// // Copyright (c) reifydb.com 2025
// // This file is licensed under the AGPL-3.0-or-later
// 
// // #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// // #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// // #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]
// 
// use reifydb_cli::cli;
// 
// fn main() {
//     let args: Vec<String> = std::env::args().collect();
//     if let Err(err) = cli(args) {
//         println!("{err:?}");
//         std::process::exit(1)
//     }
// }


// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: String,
    #[serde(rename = "type")]
    msg_type: String,
    payload: serde_json::Value,
}

#[tokio::main]
async fn main() {
    let url = url::Url::parse("ws://127.0.0.1:9001").unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let query = Message {
        id: "req1".into(),
        msg_type: "query".into(),
        payload: json!({ "statement": "from trades" }),
    };

    let query_str = serde_json::to_string(&query).unwrap();
    ws_stream.send(WsMessage::Text(query_str)).await.unwrap();
    

    while let Some(Ok(msg)) = ws_stream.next().await {
        if msg.is_text() {
            let resp: Message = serde_json::from_str(&msg.into_text().unwrap()).unwrap();
            println!("Received: {:?}", resp);
        }
    }
}