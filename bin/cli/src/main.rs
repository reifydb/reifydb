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

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Serialize, Deserialize)]
struct ClientMessage {
    id: String,
    #[serde(rename = "type")]
    msg_type: String,
    payload: serde_json::Value,
}

#[tokio::main]
async fn main() {
    let url = url::Url::parse("ws://127.0.0.1:9001").unwrap();
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let auth_msg = ClientMessage {
        id: "auth-1".to_string(),
        msg_type: "Auth".to_string(),
        payload: json!({ "access_token": "mysecrettoken" }),
    };

    ws_stream.send(Message::Text(serde_json::to_string(&auth_msg).unwrap())).await.unwrap();

    println!("✅ Sent auth message");

    let query_msg = ClientMessage {
        id: "req-1".to_string(),
        msg_type: "Query".to_string(),
        payload: json!({ "statement": "from trades" }),
    };

    ws_stream.send(Message::Text(serde_json::to_string(&query_msg).unwrap())).await.unwrap();

    println!("📤 Sent query");

    while let Some(Ok(msg)) = ws_stream.next().await {
        if msg.is_text() {
            println!("📥 Received: {}", msg.to_text().unwrap());
        }
    }
}
