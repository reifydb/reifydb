// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod rx;

use crate::ws::{
    AuthRequest, Request, RequestPayload, Response, ResponsePayload, ReadRequest, ReadResponse,
    WriteRequest, WriteResponse,
};
use futures_util::{SinkExt, StreamExt};
use reifydb_core::result::error::diagnostic::Diagnostic;
use reifydb_core::result::{Frame, FrameColumn, FrameColumnData};
use reifydb_core::value::Blob;
use reifydb_core::value::container::{
    BlobContainer, BoolContainer, NumberContainer, RowIdContainer, StringContainer,
    TemporalContainer, UndefinedContainer, UuidContainer,
};
use reifydb_core::value::temporal::parse_interval;
use reifydb_core::{Date, DateTime, Error, Interval, OwnedSpan, RowId, Time, Type, err};
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
                cause: None,
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
        self.tx.send(Request { id, payload: RequestPayload::Auth(AuthRequest { token }) }).unwrap();

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

    pub async fn execute(&self, statement: &str) -> Result<WriteResponse, Error> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);

        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Write(WriteRequest { statements: vec![statement.to_string()] }),
            })
            .unwrap();

        let resp = timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .expect("Execute response channel dropped");

        match resp.payload {
            ResponsePayload::Write(payload) => Ok(payload),
            ResponsePayload::Err(payload) => err!(payload.diagnostic),
            other => {
                eprintln!("Unexpected execute response: {:?}", other);
                panic!("Unexpected execute response type")
            }
        }
    }

    pub async fn query(&self, statement: &str) -> Result<ReadResponse, Error> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);

        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Read(ReadRequest { statements: vec![statement.to_string()] }),
            })
            .unwrap();

        let resp = timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .expect("Query response channel dropped");

        match resp.payload {
            ResponsePayload::Read(payload) => Ok(payload),
            ResponsePayload::Err(payload) => err!(payload.diagnostic),
            other => {
                eprintln!("Unexpected query response: {:?}", other);
                panic!("Unexpected query response type")
            }
        }
    }

    pub async fn write(&self, statement: &str) -> Result<Vec<Frame>, Error> {
        let response = self.execute(statement).await?;
        Ok(convert_execute_response(response))
    }

    pub async fn read(&self, statement: &str) -> Result<Vec<Frame>, Error> {
        let response = self.query(statement).await?;
        Ok(convert_query_response(response))
    }
}

fn convert_execute_response(payload: WriteResponse) -> Vec<Frame> {
    let mut result = Vec::new();

    for frame in payload.frames {
        let columns = frame
            .columns
            .into_iter()
            .enumerate()
            .map(|(_i, col)| FrameColumn {
                schema: None,
                table: col.frame,
                name: col.name,
                data: convert_column_values(col.ty, col.data),
            })
            .collect();

        result.push(Frame::new(columns))
    }

    result
}

fn convert_query_response(payload: ReadResponse) -> Vec<Frame> {
    let mut result = Vec::new();

    for frame in payload.frames {
        let columns = frame
            .columns
            .into_iter()
            .enumerate()
            .map(|(_i, col)| FrameColumn {
                schema: None,
                table: col.frame,
                name: col.name,
                data: convert_column_values(col.ty, col.data),
            })
            .collect();

        result.push(Frame::new(columns))
    }

    result
}

/// Parse interval from ISO 8601 duration string using core parser (eliminates approximation)
fn parse_interval_string(s: &str) -> Result<Interval, ()> {
    let span = OwnedSpan::testing(s);
    parse_interval(span).map_err(|_| ())
}

fn convert_column_values(target: Type, data: Vec<String>) -> FrameColumnData {
    let bitvec: Vec<bool> = data.iter().map(|s| s != "⟪undefined⟫").collect();

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
            FrameColumnData::$variant(NumberContainer::new(values, bitvec.into()))
        }};
    }

    match target {
        Type::Bool => {
            let values: Vec<bool> = data
                .iter()
                .map(|s| match s.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => false, // treat ⟪undefined⟫ or anything else as false
                })
                .collect();
            FrameColumnData::Bool(BoolContainer::new(values, bitvec.into()))
        }
        Type::Float4 => parse!(f32, Float4),
        Type::Float8 => parse!(f64, Float8),
        Type::Int1 => parse!(i8, Int1),
        Type::Int2 => parse!(i16, Int2),
        Type::Int4 => parse!(i32, Int4),
        Type::Int8 => parse!(i64, Int8),
        Type::Int16 => parse!(i128, Int16),
        Type::Uint1 => parse!(u8, Uint1),
        Type::Uint2 => parse!(u16, Uint2),
        Type::Uint4 => parse!(u32, Uint4),
        Type::Uint8 => parse!(u64, Uint8),
        Type::Uint16 => parse!(u128, Uint16),
        Type::Utf8 => {
            let values: Vec<String> = data
                .iter()
                .map(|s| if s == "⟪undefined⟫" { "".to_string() } else { s.clone() })
                .collect();
            FrameColumnData::Utf8(StringContainer::new(values, bitvec.into()))
        }
        Type::Date => {
            let values: Vec<Date> = data
                .iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        Date::default()
                    } else {
                        // Parse date from ISO format (YYYY-MM-DD)
                        let parts: Vec<&str> = s.split('-').collect();
                        if parts.len() == 3 {
                            let year = parts[0].parse::<i32>().unwrap_or(1970);
                            let month = parts[1].parse::<u32>().unwrap_or(1);
                            let day = parts[2].parse::<u32>().unwrap_or(1);
                            Date::from_ymd(year, month, day).unwrap_or_default()
                        } else {
                            Date::default()
                        }
                    }
                })
                .collect();
            FrameColumnData::Date(TemporalContainer::new(values, bitvec.into()))
        }
        Type::DateTime => {
            let values: Vec<DateTime> = data
                .iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        DateTime::default()
                    } else {
                        // Try parsing as timestamp first
                        if let Ok(timestamp) = s.parse::<i64>() {
                            DateTime::from_timestamp(timestamp).unwrap_or_default()
                        } else {
                            // Try parsing as ISO 8601 format with RFC3339 (handles Z suffix)
                            match chrono::DateTime::parse_from_rfc3339(s) {
                                Ok(dt) => DateTime::from_chrono_datetime(dt.with_timezone(&chrono::Utc)),
                                Err(_) => {
                                    // Try parsing without timezone (assume UTC)
                                    match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                                        Ok(ndt) => DateTime::from_chrono_datetime(ndt.and_utc()),
                                        Err(_) => {
                                            // Try parsing with fractional seconds
                                            match chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                                                Ok(ndt) => DateTime::from_chrono_datetime(ndt.and_utc()),
                                                Err(_) => {
                                                    // Try parsing with Z suffix manually
                                                    let clean_str = if s.ends_with('Z') {
                                                        &s[..s.len()-1]
                                                    } else {
                                                        s
                                                    };
                                                    match chrono::NaiveDateTime::parse_from_str(clean_str, "%Y-%m-%dT%H:%M:%S") {
                                                        Ok(ndt) => DateTime::from_chrono_datetime(ndt.and_utc()),
                                                        Err(_) => {
                                                            // Try with fractional seconds and Z suffix
                                                            match chrono::NaiveDateTime::parse_from_str(clean_str, "%Y-%m-%dT%H:%M:%S%.f") {
                                                                Ok(ndt) => DateTime::from_chrono_datetime(ndt.and_utc()),
                                                                Err(_) => DateTime::default()
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .collect();
            FrameColumnData::DateTime(TemporalContainer::new(values, bitvec.into()))
        }
        Type::Time => {
            let values: Vec<Time> = data
                .iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        Time::default()
                    } else {
                        // Parse time from HH:MM:SS.nnnnnnnnn format
                        let parts: Vec<&str> = s.split(':').collect();
                        if parts.len() >= 3 {
                            let hour = parts[0].parse::<u32>().unwrap_or(0);
                            let min = parts[1].parse::<u32>().unwrap_or(0);

                            // Handle seconds and nanoseconds
                            let sec_parts: Vec<&str> = parts[2].split('.').collect();
                            let sec = sec_parts[0].parse::<u32>().unwrap_or(0);

                            let nano = if sec_parts.len() > 1 {
                                let frac_str = sec_parts[1];
                                let padded = if frac_str.len() < 9 {
                                    format!("{:0<9}", frac_str)
                                } else {
                                    frac_str[..9].to_string()
                                };
                                padded.parse::<u32>().unwrap_or(0)
                            } else {
                                0
                            };

                            Time::from_hms_nano(hour, min, sec, nano).unwrap_or_default()
                        } else {
                            Time::default()
                        }
                    }
                })
                .collect();
            FrameColumnData::Time(TemporalContainer::new(values, bitvec.into()))
        }
        Type::Interval => {
            let values: Vec<Interval> = data
                .iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        Interval::default()
                    } else {
                        // Parse interval from ISO 8601 duration string (e.g., P1D, PT2H30M, P428DT4H5M6S)
                        parse_interval_string(s).unwrap_or_default()
                    }
                })
                .collect();
            FrameColumnData::Interval(TemporalContainer::new(values, bitvec.into()))
        }
        Type::Undefined => FrameColumnData::Undefined(UndefinedContainer::new(data.len())),
        Type::RowId => {
            let values: Vec<_> = data
                .into_iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        RowId::default()
                    } else {
                        if let Ok(id) = s.parse::<u64>() {
                            RowId::new(id)
                        } else {
                            RowId::default()
                        }
                    }
                })
                .collect();
            FrameColumnData::RowId(RowIdContainer::new(values, bitvec.into()))
        }
        Type::Uuid4 => {
            let values: Vec<reifydb_core::value::uuid::Uuid4> = data
                .into_iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        reifydb_core::value::uuid::Uuid4::from(Uuid::nil())
                    } else {
                        reifydb_core::value::uuid::Uuid4::from(
                            Uuid::parse_str(&s).unwrap_or(Uuid::nil()),
                        )
                    }
                })
                .collect();
            FrameColumnData::Uuid4(UuidContainer::new(values, bitvec.into()))
        }
        Type::Uuid7 => {
            let values: Vec<reifydb_core::value::uuid::Uuid7> = data
                .into_iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        reifydb_core::value::uuid::Uuid7::from(Uuid::nil())
                    } else {
                        reifydb_core::value::uuid::Uuid7::from(
                            Uuid::parse_str(&s).unwrap_or(Uuid::nil()),
                        )
                    }
                })
                .collect();
            FrameColumnData::Uuid7(UuidContainer::new(values, bitvec.into()))
        }
        Type::Blob => {
            let values: Vec<Blob> = data
                .into_iter()
                .map(|s| {
                    if s == "⟪undefined⟫" {
                        Blob::new(vec![])
                    } else {
                        // Parse hex string (assuming 0x prefix)
                        if s.starts_with("0x") {
                            if let Ok(bytes) = hex::decode(&s[2..]) {
                                Blob::new(bytes)
                            } else {
                                Blob::new(vec![])
                            }
                        } else {
                            Blob::new(vec![])
                        }
                    }
                })
                .collect();
            FrameColumnData::Blob(BlobContainer::new(values, bitvec.into()))
        }
    }
}
