// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod rx;

use crate::ws::{
    AuthRequest, Request, RequestPayload, Response, ResponsePayload, RxRequest, RxResponse,
    TxRequest, TxResponse,
};
use futures_util::{SinkExt, StreamExt};
use reifydb_core::diagnostic::Diagnostic;
use reifydb_core::{CowVec, Date, DateTime, Error, Interval, Time, Type};
use reifydb_engine::frame::{ColumnValues, Frame, FrameColumn};
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
                caused_by: None,
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

    pub async fn execute(&self, statement: &str) -> Result<TxResponse, Error> {
        let id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.pending.lock().await.insert(id.clone(), tx);

        self.tx
            .send(Request {
                id,
                payload: RequestPayload::Tx(TxRequest { statements: vec![statement.to_string()] }),
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
                payload: RequestPayload::Rx(RxRequest { statements: vec![statement.to_string()] }),
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
                FrameColumn { name: col.name, values: convert_column_values(col.ty, col.data) }
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
                FrameColumn { name: col.name, values: convert_column_values(col.ty, col.data) }
            })
            .collect();

        result.push(Frame { name: frame.name, columns, index })
    }

    result
}

// FIXME this is duplicated - move this into a the core crate - like promote, demote, convert
/// Parse interval from ISO 8601 duration string (e.g., P1D, PT2H30M, P428DT4H5M6S)
fn parse_interval_string(s: &str) -> Result<Interval, ()> {
    if s.len() < 2 || !s.starts_with('P') {
        return Err(());
    }

    let mut chars = s.chars().skip(1); // Skip 'P'
    let mut total_nanos = 0i64;
    let mut current_number = String::new();
    let mut in_time_part = false;

    while let Some(c) = chars.next() {
        match c {
            'T' => {
                in_time_part = true;
            }
            '0'..='9' => {
                current_number.push(c);
            }
            'Y' => {
                if in_time_part || current_number.is_empty() {
                    return Err(());
                }
                let years: i64 = current_number.parse().map_err(|_| ())?;
                total_nanos += years * 365 * 24 * 60 * 60 * 1_000_000_000; // Approximate
                current_number.clear();
            }
            'M' => {
                if current_number.is_empty() {
                    return Err(());
                }
                let value: i64 = current_number.parse().map_err(|_| ())?;
                if in_time_part {
                    total_nanos += value * 60 * 1_000_000_000; // Minutes
                } else {
                    total_nanos += value * 30 * 24 * 60 * 60 * 1_000_000_000; // Months (approximate)
                }
                current_number.clear();
            }
            'W' => {
                if in_time_part || current_number.is_empty() {
                    return Err(());
                }
                let weeks: i64 = current_number.parse().map_err(|_| ())?;
                total_nanos += weeks * 7 * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'D' => {
                if in_time_part || current_number.is_empty() {
                    return Err(());
                }
                let days: i64 = current_number.parse().map_err(|_| ())?;
                total_nanos += days * 24 * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'H' => {
                if !in_time_part || current_number.is_empty() {
                    return Err(());
                }
                let hours: i64 = current_number.parse().map_err(|_| ())?;
                total_nanos += hours * 60 * 60 * 1_000_000_000;
                current_number.clear();
            }
            'S' => {
                if !in_time_part || current_number.is_empty() {
                    return Err(());
                }
                let seconds: i64 = current_number.parse().map_err(|_| ())?;
                total_nanos += seconds * 1_000_000_000;
                current_number.clear();
            }
            _ => {
                return Err(());
            }
        }
    }

    if !current_number.is_empty() {
        return Err(());
    }

    Ok(Interval::from_nanos(total_nanos))
}

fn convert_column_values(ty: Type, data: Vec<String>) -> ColumnValues {
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

    match ty {
        Type::Bool => {
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
            ColumnValues::Utf8(CowVec::new(values), CowVec::new(validity))
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
            ColumnValues::Date(CowVec::new(values), CowVec::new(validity))
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
            ColumnValues::DateTime(CowVec::new(values), CowVec::new(validity))
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
            ColumnValues::Time(CowVec::new(values), CowVec::new(validity))
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
            ColumnValues::Interval(CowVec::new(values), CowVec::new(validity))
        }
        Type::Undefined => ColumnValues::Undefined(data.len()),
    }
}
