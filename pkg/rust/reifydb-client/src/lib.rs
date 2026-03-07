// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "http")]
pub mod http;
#[cfg(any(feature = "http", feature = "ws"))]
mod session;
#[cfg(feature = "ws")]
mod utils;
#[cfg(feature = "ws")]
pub mod ws;

// Re-export client types
#[cfg(feature = "grpc")]
pub use grpc::{GrpcClient, GrpcSubscription};
#[cfg(feature = "http")]
pub use http::HttpClient;
// Re-export derive macro
pub use reifydb_client_derive::FromFrame;
// Re-export commonly used types from reifydb-type
pub use reifydb_type as r#type;
#[cfg(any(feature = "http", feature = "ws"))]
use reifydb_type::error::Diagnostic;
pub use reifydb_type::{
	params::Params,
	value::{
		Value,
		frame::{
			column::FrameColumn,
			data::FrameColumnData,
			extract::FrameError,
			frame::Frame,
			from_frame::FromFrameError,
			row::{FrameRow, FrameRows},
		},
		ordered_f32::OrderedF32,
		ordered_f64::OrderedF64,
		try_from::{FromValueError, TryFromValue, TryFromValueCoerce},
		r#type::Type,
	},
};
#[cfg(any(feature = "http", feature = "ws"))]
use serde::{Deserialize, Serialize};
#[cfg(feature = "ws")]
pub use ws::WsClient;

/// Result type for admin operations
#[derive(Debug)]
pub struct AdminResult {
	pub frames: Vec<Frame>,
}

/// Result type for command operations
#[derive(Debug)]
pub struct CommandResult {
	pub frames: Vec<Frame>,
}

/// Result type for query operations
#[derive(Debug)]
pub struct QueryResult {
	pub frames: Vec<Frame>,
}

#[cfg(any(feature = "http", feature = "ws"))]
/// Wire format for a single typed value: `{"type": "Int2", "value": "1234"}`.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireValue {
	#[serde(rename = "type")]
	pub type_name: String,
	pub value: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
/// Wire format for query parameters.
///
/// Either positional or named:
/// - Positional: `[{"type":"Int2","value":"1234"}, ...]`
/// - Named: `{"key": {"type":"Int2","value":"1234"}, ...}`
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireParams {
	Positional(Vec<WireValue>),
	Named(std::collections::HashMap<String, WireValue>),
}

#[cfg(any(feature = "http", feature = "ws"))]
fn value_to_wire(value: Value) -> WireValue {
	let (type_name, value_str): (&str, String) = match &value {
		Value::None {
			..
		} => ("None", "\u{27EA}none\u{27EB}".to_string()),
		Value::Boolean(b) => ("Boolean", b.to_string()),
		Value::Float4(f) => ("Float4", f.to_string()),
		Value::Float8(f) => ("Float8", f.to_string()),
		Value::Int1(i) => ("Int1", i.to_string()),
		Value::Int2(i) => ("Int2", i.to_string()),
		Value::Int4(i) => ("Int4", i.to_string()),
		Value::Int8(i) => ("Int8", i.to_string()),
		Value::Int16(i) => ("Int16", i.to_string()),
		Value::Utf8(s) => ("Utf8", s.clone()),
		Value::Uint1(u) => ("Uint1", u.to_string()),
		Value::Uint2(u) => ("Uint2", u.to_string()),
		Value::Uint4(u) => ("Uint4", u.to_string()),
		Value::Uint8(u) => ("Uint8", u.to_string()),
		Value::Uint16(u) => ("Uint16", u.to_string()),
		Value::Uuid4(u) => ("Uuid4", u.to_string()),
		Value::Uuid7(u) => ("Uuid7", u.to_string()),
		Value::Date(d) => ("Date", d.to_string()),
		Value::DateTime(dt) => ("DateTime", dt.to_string()),
		Value::Time(t) => ("Time", t.to_string()),
		Value::Duration(d) => ("Duration", d.to_string()),
		Value::Blob(b) => ("Blob", b.to_hex()),
		Value::IdentityId(id) => ("IdentityId", id.to_string()),
		Value::Int(i) => ("Int", i.to_string()),
		Value::Uint(u) => ("Uint", u.to_string()),
		Value::Decimal(d) => ("Decimal", d.to_string()),
		Value::Any(v) => return value_to_wire(*v.clone()),
		Value::DictionaryId(id) => ("DictionaryId", id.to_string()),
		Value::Type(t) => ("Type", t.to_string()),
		Value::List(items) => ("List", format!("{}", Value::List(items.clone()))),
		Value::Record(fields) => ("Record", format!("{}", Value::Record(fields.clone()))),
		Value::Tuple(items) => ("Tuple", format!("{}", Value::Tuple(items.clone()))),
	};
	WireValue {
		type_name: type_name.to_string(),
		value: value_str,
	}
}

#[cfg(any(feature = "http", feature = "ws"))]
pub fn params_to_wire(params: Params) -> Option<WireParams> {
	match params {
		Params::None => None,
		Params::Positional(values) => {
			Some(WireParams::Positional(values.into_iter().map(value_to_wire).collect()))
		}
		Params::Named(map) => {
			Some(WireParams::Named(map.into_iter().map(|(k, v)| (k, value_to_wire(v))).collect()))
		}
	}
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Admin(AdminRequest),
	Command(CommandRequest),
	Query(QueryRequest),
	Subscribe(SubscribeRequest),
	Unsubscribe(UnsubscribeRequest),
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	pub query: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ResponsePayload {
	Auth(AuthResponse),
	Err(ErrResponse),
	Admin(AdminResponse),
	Command(CommandResponse),
	Query(QueryResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminResponse {
	pub content_type: String,
	pub body: serde_json::Value,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
	pub content_type: String,
	pub body: serde_json::Value,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
	pub content_type: String,
	pub body: serde_json::Value,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribedResponse {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribedResponse {
	pub subscription_id: String,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<WebsocketColumn>,
}

#[cfg(any(feature = "http", feature = "ws"))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}

#[cfg(any(feature = "http", feature = "ws"))]
/// Server-initiated push message (no request id).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(ChangePayload),
}

#[cfg(any(feature = "http", feature = "ws"))]
/// Payload for subscription change notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePayload {
	pub subscription_id: String,
	pub content_type: String,
	pub body: serde_json::Value,
}
