// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod rx;

use crate::ws::{
    AuthRequest, TxRequest, TxResponse, RxRequest,
    RxResponse, Request, RequestPayload, Response, ResponsePayload,
};
use futures_util::{SinkExt, StreamExt};
use reifydb_core::{CowVec, Diagnostic, Error, DataType};
use reifydb_engine::frame::{Column, ColumnValues, Frame};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::time::timeout;
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
    is_closed: Arc<AtomicBool>,
}

impl Drop for WsClient {
    fn drop(&mut self) {
        self.close();
    }
}

impl WsClient {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let (ws_stream, _) = connect_async(url).await.map_err(|_| {
            Error(Diagnostic {
                code: "TBD".to_string(),
                statement: None,
                message: "TBD".to_string(),
                column: None,
                span: None,
                label: None,
                help: None,
                notes: vec![],
            })
        })?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let pending = Arc::new(Mutex::new(HashMap::<String, oneshot::Sender<Response>>::new()));
        let stream_pending = pending.clone();
        let is_closed = Arc::new(AtomicBool::new(false));
        let close_flag = is_closed.clone();

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

            close_flag.store(true, Ordering::SeqCst);
        });

        Ok(Self { tx, pending, is_closed })
    }

    pub fn close(&self) {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            // Dropping the sender signals the background task to shut down
        }
    }

    pub async fn auth(&self, token: Option<String>) -> Result<(), WsError> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);
        self.tx
            .send(Request { id, payload: RequestPayload::Auth(AuthRequest { token }) })
            .unwrap();

        let resp = timeout(Duration::from_secs(3), rx)
            .await
            .unwrap()
            .expect("Auth response channel dropped");

        match resp.payload {
            ResponsePayload::Auth(_) => Ok(()),
            other => {
                eprintln!("Unexpected auth response: {:?}", other);
                panic!("Unexpected query response type")
            }
        }
    }

    pub async fn execute(&self, statement: &str) -> Result<TxResponse, Error> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);

        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Tx(TxRequest {
                    statements: vec![statement.to_string()],
                }),
            })
            .unwrap();

        let resp = timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .expect("Execute response channel dropped");

        match resp.payload {
            ResponsePayload::Tx(payload) => Ok(payload),
            ResponsePayload::Err(payload) => Err(Error(payload.diagnostic)),
            other => {
                eprintln!("Unexpected execute response: {:?}", other);
                panic!("Unexpected execute response type")
            }
        }
    }

    pub async fn query(&self, statement: &str) -> Result<RxResponse, Error> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);

        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Rx(RxRequest {
                    statements: vec![statement.to_string()],
                }),
            })
            .unwrap();

        let resp = timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .expect("Query response channel dropped");

        match resp.payload {
            ResponsePayload::Rx(payload) => Ok(payload),
            ResponsePayload::Err(payload) => Err(Error(payload.diagnostic)),
            other => {
                eprintln!("Unexpected query response: {:?}", other);
                panic!("Unexpected query response type")
            }
        }
    }

    pub async fn tx(&self, statement: &str) -> Result<Vec<Frame>, Error> {
        let response = self.execute(statement).await?;
        Ok(convert_execute_response(response))
    }

    pub async fn rx(&self, statement: &str) -> Result<Vec<Frame>, Error> {
        let response = self.query(statement).await?;
        Ok(convert_query_response(response))
    }
}

fn convert_execute_response(payload: TxResponse) -> Vec<Frame> {
    let mut result = Vec::new();

    for frame in payload.frames {
        let mut index = HashMap::new();
        let columns = frame
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, col)| {
                index.insert(col.name.clone(), i);
                Column { name: col.name, values: convert_column_values(col.data_type, col.data) }
            })
            .collect();

        result.push(Frame { name: frame.name, columns, index })
    }

    result
}

fn convert_query_response(payload: RxResponse) -> Vec<Frame> {
    let mut result = Vec::new();

    for frame in payload.frames {
        let mut index = HashMap::new();
        let columns = frame
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, col)| {
                index.insert(col.name.clone(), i);
                Column { name: col.name, values: convert_column_values(col.data_type, col.data) }
            })
            .collect();

        result.push(Frame { name: frame.name, columns, index })
    }

    result
}

fn convert_column_values(data_type: DataType, data: Vec<String>) -> ColumnValues {
    let validity: Vec<bool> = data.iter().map(|s| s != "⟪undefined⟫").collect();

    macro_rules! parse {
        ($typ:ty, $variant:ident) => {{
            let values: Vec<$typ> = data
                .iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        Default::default()
                    } else {
                        s.parse::<$typ>().unwrap_or_default()
                    }
                })
                .collect();
            ColumnValues::$variant(CowVec::new(values), CowVec::new(validity))
        }};
    }

    match data_type {
        DataType::Bool => {
            let values: Vec<bool> = data
                .iter()
                .map(|s| match s.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => false, // treat ⟪undefined⟫ or anything else as false
                })
                .collect();
            ColumnValues::Bool(CowVec::new(values), CowVec::new(validity))
        }
        DataType::Float4 => parse!(f32, Float4),
        DataType::Float8 => parse!(f64, Float8),
        DataType::Int1 => parse!(i8, Int1),
        DataType::Int2 => parse!(i16, Int2),
        DataType::Int4 => parse!(i32, Int4),
        DataType::Int8 => parse!(i64, Int8),
        DataType::Int16 => parse!(i128, Int16),
        DataType::Uint1 => parse!(u8, Uint1),
        DataType::Uint2 => parse!(u16, Uint2),
        DataType::Uint4 => parse!(u32, Uint4),
        DataType::Uint8 => parse!(u64, Uint8),
        DataType::Uint16 => parse!(u128, Uint16),
        DataType::Utf8 => {
            let values: Vec<String> = data
                .iter()
                .map(|s| if s == "⟪undefined⟫" { "".to_string() } else { s.clone() })
                .collect();
            ColumnValues::Utf8(CowVec::new(values), CowVec::new(validity))
        }
        DataType::Undefined => ColumnValues::Undefined(data.len()),
    }
}
