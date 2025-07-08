// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ws::RequestPayload::Auth;
use crate::ws::{
    AuthRequestPayload, AuthResponsePayload, ExecuteRequestPayload, ExecuteResponsePayload,
    QueryRequestPayload, QueryResponsePayload, Request, RequestPayload, ResponsePayload,
    WebsocketColumn, WebsocketFrame,
};
use futures_util::{SinkExt, StreamExt};
use reifydb_core::Value;
use reifydb_core::interface::{Principal, Transaction, UnversionedStorage, VersionedStorage};
use reifydb_engine::Engine;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Utf8Bytes;

use tokio_tungstenite::tungstenite::Message as WsMessage;

#[derive(Debug)]
pub struct WsConfig {
    pub socket: Option<SocketAddr>,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self { socket: Some("[::1]:9001".parse().unwrap()) }
    }
}

pub struct WsServer<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    config: WsConfig,
    engine: Engine<VS, US, T>,
    shutdown: Arc<Notify>,
}

impl<VS, US, T> WsServer<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn close(&self) {
        self.shutdown.notify_waiters();
    }
}

impl<VS, US, T> WsServer<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(config: WsConfig, engine: Engine<VS, US, T>) -> Self {
        Self { config, engine, shutdown: Arc::new(Notify::new()) }
    }

    pub async fn serve(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let listener =
            TcpListener::bind(self.config.socket.unwrap_or("[::1]:9001".parse().unwrap()))
                .await
                .unwrap();

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    println!("🛑 Shutting down WebSocket server");
                    return Ok(());
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let engine = self.engine.clone();
                            Self::handle(engine, stream, self.shutdown.clone()).await;
                        }
                        Err(e) => {
                            eprintln!("❌ Accept error: {e}");
                            continue;
                        }
                    }
                }
            }
        }
    }

    fn validate_token(token: &str) -> bool {
        token == "mysecrettoken"
    }

    async fn handle(engine: Engine<VS, US, T>, stream: TcpStream, shutdown: Arc<Notify>) {
        tokio::spawn(async move {
            let ws_stream = accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws_stream.split();

            let Some(Ok(WsMessage::Text(text))) = read.next().await else {
                eprintln!("❌ No valid first message");
                return;
            };

            match serde_json::from_str::<crate::ws::request::Request>(&text) {
                Ok(request) => match request.payload {
                    Auth(AuthRequestPayload { token: Some(token) }) => {
                        if Self::validate_token(&token) {
                            println!("✅ Authenticated: {}", token);

                            let response = crate::ws::response::Response {
                                id: request.id,
                                payload: ResponsePayload::Auth(AuthResponsePayload {}),
                            };

                            let msg = serde_json::to_string(&response).unwrap();
                            if write.send(WsMessage::Text(Utf8Bytes::from(msg))).await.is_err() {
                                return;
                            }

                            loop {
                                tokio::select! {
                                    _ = shutdown.notified() => {
                                        println!("🔌 Shutdown signal received: closing connection");
                                        let _ = write.send(WsMessage::Close(None)).await;
                                        break;
                                    }
                                    msg = read.next() => {
                                        match msg {
                                            Some(Ok(WsMessage::Text(text))) => {
                                                match serde_json::from_str::<Request>(&text) {
                                                    Ok(request) => match request.payload {
                                                          RequestPayload::Execute(ExecuteRequestPayload { statements }) => {
                                                            println!("📥 Received query: {}", statements.join(","));

                                                            if let Some(statement) = statements.first() {
                                                                match engine.execute_as(
                                                                    &Principal::System { id: 1, name: "root".to_string() },
                                                                    statement,
                                                                ) {
                                                                    Ok(result) => {
                                                                        let response = crate::ws::response::Response {
                                                                            id: request.id,
                                                                            payload: ResponsePayload::Execute(ExecuteResponsePayload {
                                                                                frames: result.into_iter().map(|frame| WebsocketFrame {
                                                                                    name: frame.name,
                                                                                    columns: frame.columns.into_iter().map(|c| WebsocketColumn {
                                                                                        name: c.name.clone(),
                                                                                        kind: c.kind(),
                                                                                        data: c.values.iter().map(|v| {
                                                                                            if v == Value::Undefined {
                                                                                                "⟪undefined⟫".to_string()
                                                                                            } else {
                                                                                                v.to_string()
                                                                                            }
                                                                                        }).collect(),
                                                                                    }).collect(),
                                                                                }).collect(),
                                                                            }),
                                                                        };

                                                                        let msg = serde_json::to_string(&response).unwrap();
                                                                        let _ = write.send(WsMessage::Text(Utf8Bytes::from(msg))).await;
                                                                    }
                                                                    Err(e) => {
                                                                        eprintln!("❌ Query error: {}", e);
                                                                    }
                                                                }
                                                            }
                                                        }

                                                        RequestPayload::Query(QueryRequestPayload { statements }) => {
                                                            println!("📥 Received query: {}", statements.join(","));

                                                            if let Some(statement) = statements.first() {
                                                                match engine.query_as(
                                                                    &Principal::System { id: 1, name: "root".to_string() },
                                                                    statement,
                                                                ) {
                                                                    Ok(result) => {
                                                                        let response = crate::ws::response::Response {
                                                                            id: request.id,
                                                                            payload: ResponsePayload::Query(QueryResponsePayload {
                                                                                frames: result.into_iter().map(|frame| WebsocketFrame {
                                                                                    name: frame.name,
                                                                                    columns: frame.columns.into_iter().map(|c| WebsocketColumn {
                                                                                        name: c.name.clone(),
                                                                                        kind: c.kind(),
                                                                                        data: c.values.iter().map(|v| {
                                                                                            if v == Value::Undefined {
                                                                                                "⟪undefined⟫".to_string()
                                                                                            } else {
                                                                                                v.to_string()
                                                                                            }
                                                                                        }).collect(),
                                                                                    }).collect(),
                                                                                }).collect(),
                                                                            }),
                                                                        };

                                                                        let msg = serde_json::to_string(&response).unwrap();
                                                                        let _ = write.send(WsMessage::Text(Utf8Bytes::from(msg))).await;
                                                                    }
                                                                    Err(e) => {
                                                                        eprintln!("❌ Query error: {}", e);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        _ => {}
                                                    },
                                                    Err(err) => {
                                                        eprintln!("❌ Invalid message: {err}");
                                                    }
                                                }
                                            }
                                            Some(Ok(WsMessage::Close(_))) => {
                                                println!("👋 Client closed the connection");
                                                break;
                                            }
                                            Some(Err(e)) => {
                                                eprintln!("❌ WebSocket error: {}", e);
                                                break;
                                            }
                                            None => {
                                                println!("🔌 Client disconnected");
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        } else {
                            eprintln!("❌ Invalid token: {token}");
                            let _ = write.send(WsMessage::Close(None)).await;
                        }
                    }
                    _ => {
                        eprintln!("❌ First message must be auth");
                        let _ = write.send(WsMessage::Close(None)).await;
                    }
                },
                Err(err) => {
                    eprintln!("❌ Failed to parse auth message: {err}");
                    let _ = write.send(WsMessage::Close(None)).await;
                }
            }
        });
    }
}
