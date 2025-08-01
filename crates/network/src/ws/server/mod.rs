// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ws::RequestPayload::Auth;
use crate::ws::{
    AuthRequest, AuthResponse, ErrResponse, Request, RequestPayload, ResponsePayload, RxRequest,
    RxResponse, TxRequest, TxResponse, WebsocketColumn, WebsocketFrame,
};
use futures_util::{SinkExt, StreamExt};
use reifydb_core::interface::{
    Engine as EngineInterface, UnversionedTransaction, Principal, VersionedTransaction, UnversionedStorage, VersionedStorage,
};
use reifydb_core::{Error, Value};
use reifydb_engine::Engine;
use std::net::IpAddr::V4;
use std::net::{Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, OnceCell};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

const DEFAULT_SOCKET: SocketAddr = SocketAddr::new(V4(Ipv4Addr::new(0, 0, 0, 0)), 8090);

#[derive(Debug)]
pub struct WsConfig {
    pub socket: Option<SocketAddr>,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self { socket: Some(DEFAULT_SOCKET) }
    }
}

#[derive(Clone)]
pub struct WsServer<VS, US, T, UT>(Arc<Inner<VS, US, T, UT>>)
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction;

pub struct Inner<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    config: WsConfig,
    engine: Engine<VS, US, T, UT>,
    shutdown: Arc<Notify>,
    shutdown_complete: AtomicBool,
    socket_addr: OnceCell<SocketAddr>,
    _phantom: std::marker::PhantomData<(VS, US, T, UT)>,
}

impl<VS, US, T, UT> Deref for WsServer<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    type Target = Inner<VS, US, T, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS, US, T, UT> WsServer<VS, US, T, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    pub fn new(config: WsConfig, engine: Engine<VS, US, T, UT>) -> Self {
        Self(Arc::new(Inner {
            config,
            engine,
            shutdown: Arc::new(Notify::new()),
            shutdown_complete: AtomicBool::new(false),
            socket_addr: OnceCell::new(),
            _phantom: std::marker::PhantomData,
        }))
    }

    pub fn socket_addr(&self) -> Option<SocketAddr> {
        self.socket_addr.get().cloned()
    }

    pub async fn close(&self) -> Result<(), &'static str> {
        self.shutdown.notify_waiters();

        let start = std::time::Instant::now();
        let timeout_duration = Duration::from_secs(10);

        while !self.shutdown_complete.load(Ordering::Acquire) {
            if start.elapsed() >= timeout_duration {
                println!("WebSocket server shutdown timed out after 10 seconds");
                return Err("Shutdown timeout");
            }

            sleep(Duration::from_millis(50)).await;
        }

        println!("ws server stopped");
        Ok(())
    }

    pub async fn serve(self) -> Result<(), Error> {
        let listener =
            TcpListener::bind(self.config.socket.unwrap_or(DEFAULT_SOCKET)).await.unwrap();

        self.socket_addr.set(listener.local_addr().unwrap()).unwrap();
        println!("ws server listening on {}", listener.local_addr().unwrap());

        let mut tasks = JoinSet::new();

        loop {
            tokio::select! {
                _ = self.shutdown.notified() => {
                    println!("Notifying {} active connections to close", tasks.len());

                    let graceful_shutdown = timeout(Duration::from_secs(5), async {
                        while !tasks.is_empty() {
                            if let Some(result) = tasks.join_next().await {
                                match result {
                                    Ok(_) => {},
                                    Err(e) if e.is_cancelled() => { }// Expected when we abort tasks
                                    Err(e) => eprintln!("❌ Connection cleanup error: {}", e),
                                }
                            }
                        }
                    }).await;

                    match graceful_shutdown {
                        Ok(_) => println!("All connections closed gracefully"),
                        Err(_) => {
                            println!("Graceful shutdown timed out, aborting remaining connections");
                            tasks.abort_all();

                            // Wait for aborted tasks to clean up
                            while let Some(result) = tasks.join_next().await {
                                if let Err(e) = result {
                                    if !e.is_cancelled() {
                                        eprintln!("❌ Connection abort error: {}", e);
                                    }
                                }
                            }
                        }
                    }

                     self.shutdown_complete.store(true, Ordering::Release);
                    return Ok(());
                }

                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _addr)) => {
                            let engine = self.engine.clone();
                            let shutdown = self.shutdown.clone();

                            tasks.spawn(async move {
                                Self::handle(engine, stream, shutdown).await;
                            });
                        }
                        Err(e) => {
                            eprintln!("❌ Accept error: {e}");
                            continue;
                        }
                    }
                }

                Some(result) = tasks.join_next() => {
                    match result {
                        Ok(_) => {
                            // Connection completed successfully
                        }
                        Err(e) => {
                            eprintln!("❌ Connection task error: {}", e);
                        }
                    }
                }
            }
        }
    }

    async fn handle(engine: Engine<VS, US, T, UT>, stream: TcpStream, shutdown: Arc<Notify>) {
        let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());

        let ws_stream = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                eprintln!("❌ WebSocket handshake failed for {}: {}", peer_addr, e);
                return;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        let auth_result = tokio::select! {
            _ = shutdown.notified() => {
                println!("🔌 Shutdown signal received during auth for {}", peer_addr);
                let _ = write.send(Message::Close(None)).await;
                return;
            }
            msg = read.next() => msg
        };

        let Some(Ok(Message::Text(text))) = auth_result else {
            eprintln!("❌ No valid first message from {}", peer_addr);
            return;
        };

        match serde_json::from_str::<Request>(&text) {
            Ok(request) => match request.payload {
                Auth(AuthRequest { token: Some(token) }) => {
                    fn validate_token(token: &str) -> bool {
                        token == "mysecrettoken"
                    }

                    if validate_token(&token) {
                        println!("Authenticated: {} from {}", token, peer_addr);

                        let response = crate::ws::response::Response {
                            id: request.id,
                            payload: ResponsePayload::Auth(AuthResponse {}),
                        };

                        let msg = serde_json::to_string(&response).unwrap();
                        if write.send(Message::Text(Utf8Bytes::from(msg))).await.is_err() {
                            return;
                        }

                        loop {
                            tokio::select! {
                            _ = shutdown.notified() => {
                                let _ = write.send(Message::Close(None)).await;
                                break;
                            }
                              msg = read.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            match serde_json::from_str::<Request>(&text) {
                                                Ok(request) => match request.payload {
                                                      RequestPayload::Tx(TxRequest { statements }) => {
                                                        println!("Tx: {}", statements.join(","));

                                                        if let Some(statement) = statements.first() {
                                                            match engine.write_as(
                                                                &Principal::System { id: 1, name: "root".to_string() },
                                                                statement,
                                                            ) {
                                                                Ok(result) => {
                                                                    let response = crate::ws::response::Response {
                                                                        id: request.id,
                                                                        payload: ResponsePayload::Tx(TxResponse {
                                                                            frames: result.into_iter().map(|frame| {
                                                                                WebsocketFrame {
                                                                                    name: "GONE".to_string(), //FIXME
                                                                                    columns: frame.into_iter().map(|c| {
                                                                                        WebsocketColumn {
                                                                                            ty: c.get_type(),
                                                                                            name: c.name.to_string(),
                                                                                            frame: c.table.as_ref().map(|s| s.to_string()),
                                                                                            data: c.iter().map(|v| {
                                                                                                if v == Value::Undefined {
                                                                                                    "⟪undefined⟫".to_string()
                                                                                                } else {
                                                                                                    v.to_string()
                                                                                                }
                                                                                            }).collect(),
                                                                                        }
                                                                                    }).collect(),
                                                                                }
                                                                            }).collect(),
                                                                        }),
                                                                    };

                                                                    let msg = serde_json::to_string(&response).unwrap();
                                                                    let _ = write.send(Message::Text(Utf8Bytes::from(msg))).await;
                                                                }
                                                                Err(e) => {
                                                                        let mut diagnostic = e.diagnostic();
                                                                        diagnostic.set_statement(statement.clone());

                                                                        let response = crate::ws::response::Response {
                                                                        id: request.id,
                                                                        payload: ResponsePayload::Err(ErrResponse {
                                                                            diagnostic
                                                                        }),
                                                                    };

                                                                    let msg = serde_json::to_string(&response).unwrap();
                                                                    let _ = write.send(Message::Text(Utf8Bytes::from(msg))).await;

                                                                    eprintln!("❌ Query error");
                                                                }
                                                            }
                                                        }
                                                    }

                                                    RequestPayload::Rx(RxRequest { statements }) => {
                                                        println!("Rx: {}", statements.join(","));

                                                        if let Some(statement) = statements.first() {
                                                            match engine.read_as(
                                                                &Principal::System { id: 1, name: "root".to_string() },
                                                                statement,
                                                            ) {
                                                                Ok(result) => {
                                                                    let response = crate::ws::response::Response {
                                                                        id: request.id,
                                                                        payload: ResponsePayload::Rx(RxResponse {
                                                                            frames: result.into_iter().map(|frame| {
                                                                                WebsocketFrame {
                                                                                    name: "GONE".to_string(), // FIXME
                                                                                    columns: frame.into_iter().map(|c| {
                                                                                        WebsocketColumn {
                                                                                            ty: c.get_type(),
                                                                                            name: c.name.to_string(),
                                                                                            frame: c.table.as_ref().map(|s| s.to_string()),
                                                                                            data: c.iter().map(|v| {
                                                                                                if v == Value::Undefined {
                                                                                                    "⟪undefined⟫".to_string()
                                                                                                } else {
                                                                                                    v.to_string()
                                                                                                }
                                                                                            }).collect(),
                                                                                        }
                                                                                    }).collect(),
                                                                                }
                                                                            }).collect(),
                                                                        }),
                                                                    };

                                                                    let msg = serde_json::to_string(&response).unwrap();
                                                                    let _ = write.send(Message::Text(Utf8Bytes::from(msg))).await;
                                                                }
                                                              Err(e) => {
                                                                        let mut diagnostic = e.diagnostic();
                                                                        diagnostic.set_statement(statement.clone());

                                                                        let response = crate::ws::response::Response {
                                                                        id: request.id,
                                                                        payload: ResponsePayload::Err(ErrResponse {
                                                                            diagnostic
                                                                        }),
                                                                    };

                                                                    let msg = serde_json::to_string(&response).unwrap();
                                                                    let _ = write.send(Message::Text(Utf8Bytes::from(msg))).await;

                                                                    eprintln!("❌ Query error");
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
                                        Some(Ok(Message::Close(_))) => {
                                            println!("Client closed the connection");
                                            break;
                                        }
                                        Some(Err(e)) => {
                                            eprintln!("❌ WebSocket error: {}", e);
                                            break;
                                        }
                                        None => {
                                            println!("Client disconnected");
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    } else {
                        eprintln!("❌ Invalid token from {}: {}", peer_addr, token);
                        let _ = write.send(Message::Close(None)).await;
                    }
                }
                _ => {
                    eprintln!("❌ First message must be auth from {}", peer_addr);
                    let _ = write.send(Message::Close(None)).await;
                }
            },
            Err(_) => todo!(),
        }
    }
}
