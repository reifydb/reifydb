// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod rx;

use crate::websocket::{
    AuthRequestPayload, QueryRequestPayload, QueryResponsePayload, Request, RequestPayload,
    Response, ResponsePayload,
};
use futures_util::{SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc, oneshot},
};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as WsError, protocol::Message},
};
use uuid::Uuid;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct WsClient {
    tx: mpsc::UnboundedSender<Request>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Response>>>>,
}

impl WsClient {
    pub async fn connect(url: &str) -> Result<Self, WsError> {
        let (ws_stream, _) = connect_async(url).await?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pending = Arc::new(Mutex::new(HashMap::<String, oneshot::Sender<Response>>::new()));
        let stream_pending = pending.clone();

        tokio::spawn(async move {
            let (mut write, mut read) = ws_stream.split();

            // Outgoing message loop
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    if let Ok(text) = serde_json::to_string(&msg) {
                        if let Err(e) = write.send(Message::Text(Utf8Bytes::from(text))).await {
                            eprintln!("❌ Send error: {e}");
                            break;
                        }
                    }
                }
            });

            // Incoming message loop
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    match serde_json::from_str::<Response>(&text) {
                        Ok(resp) => {
                            if let Some(tx) = stream_pending.lock().await.remove(&resp.id) {
                                let _ = tx.send(resp);
                            } else {
                                eprintln!("⚠️ No pending handler for id: {}", resp.id);
                            }
                        }
                        Err(e) => eprintln!("❌ Parse error: {e}"),
                    }
                }
            }
        });

        let client = Self { tx, pending };

        Ok(client)
    }

    pub async fn auth(&self, token: Option<String>) -> Result<(), WsError> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);
        self.tx
            .send(Request { id, payload: RequestPayload::Auth(AuthRequestPayload { token }) })
            .unwrap();

        let resp = rx.await.expect("Auth response channel dropped");

        match resp.payload {
            ResponsePayload::Auth(_) => Ok(()),
            other => {
                eprintln!("Unexpected auth response: {:?}", other);
                panic!("Unexpected query response type")
            }
        }
    }

    pub async fn query(&self, statement: String) -> Result<QueryResponsePayload, WsError> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);
        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Query(QueryRequestPayload { statements: vec![statement] }),
            })
            .unwrap();

        let resp = rx.await.expect("Query response channel dropped");

        match resp.payload {
            ResponsePayload::Query(payload) => Ok(payload),
            other => {
                eprintln!("Unexpected query response: {:?}", other);
                panic!("Unexpected query response type")
            }
        }
    }
}
