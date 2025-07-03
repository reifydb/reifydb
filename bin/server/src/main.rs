// use reifydb::ReifyDB;
// use reifydb::runtime::Runtime;
// use reifydb::server::{DatabaseConfig, ServerConfig};
//
// fn main() {
//     let rt = Runtime::new().unwrap();
//
//     ReifyDB::server()
//         .with_config(ServerConfig {
//             database: DatabaseConfig { socket_addr: "127.0.0.1:54321".parse().ok() },
//         })
//         .on_create(|ctx| async move {
//             ctx.tx("create schema test");
//             ctx.tx("create table test.arith(id: int2, value: int2, num: int2)");
//             ctx.tx("insert (1,1,5), (1,1,10), (1,2,15), (2,1,10), (2,1,30) into test.arith(id,value,num)");
//         })
//         .serve_blocking(rt);
// }


// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use futures::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    id: String,
    #[serde(rename = "type")]
    msg_type: String,
    payload: serde_json::Value,
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
    println!("WebSocket server running at ws://127.0.0.1:9001");

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, addr));
    }
}

async fn handle_connection(stream: tokio::net::TcpStream, addr: SocketAddr) {
    println!("Client connected: {}", addr);
    let ws_stream = accept_async(stream).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    while let Some(Ok(msg)) = read.next().await {
        if msg.is_text() {
            let raw = msg.into_text().unwrap();
            if let Ok(msg_data) = serde_json::from_str::<Message>(&raw) {
                println!("Received: {:?}", msg_data);

                // Echo with result
                let response = Message {
                    id: msg_data.id.clone(),
                    msg_type: "result".to_string(),
                    payload: serde_json::json!({
                        "result": "Query processed",
                        "original": msg_data.payload,
                    }),
                };

                let response_str = serde_json::to_string(&response).unwrap();
                write.send(response_str.into()).await.unwrap();
            }
        }
    }

    println!("Client disconnected: {}", addr);
}