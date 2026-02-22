// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB
pub mod http;
mod session;
mod utils;
pub mod ws;

// Re-export client types
pub use http::HttpClient;
// Re-export derive macro
pub use reifydb_client_derive::FromFrame;
// Re-export commonly used types from reifydb-type
pub use reifydb_type as r#type;
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
use serde::{Deserialize, Serialize};
// Re-export result types
pub use session::{AdminResult, CommandResult, QueryResult};
pub use ws::WsClient;

// ============================================================================
// Wire format types for WebSocket protocol
// ============================================================================

/// Wire format for a single typed value: `{"type": "Int2", "value": "1234"}`.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireValue {
	#[serde(rename = "type")]
	pub type_name: String,
	pub value: String,
}

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

fn value_to_wire(value: Value) -> WireValue {
	let (type_name, value_str): (&str, String) = match &value {
		Value::None { .. } => ("None", "\u{27EA}none\u{27EB}".to_string()),
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
	};
	WireValue {
		type_name: type_name.to_string(),
		value: value_str,
	}
}

pub fn params_to_wire(params: Params) -> Option<WireParams> {
	match params {
		Params::None => None,
		Params::Positional(values) => {
			Some(WireParams::Positional(values.into_iter().map(value_to_wire).collect()))
		}
		Params::Named(map) => {
			Some(WireParams::Named(
				map.into_iter().map(|(k, v)| (k, value_to_wire(v))).collect(),
			))
		}
	}
}

// ============================================================================
// Request Types (matching server)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub statements: Vec<String>,
	pub params: Option<WireParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	pub query: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	pub subscription_id: String,
}

// ============================================================================
// Response Types (matching server)
// ============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
	pub id: String,
	#[serde(flatten)]
	pub payload: ResponsePayload,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminResponse {
	pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
	pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
	pub frames: Vec<WebsocketFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribedResponse {
	pub subscription_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribedResponse {
	pub subscription_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}

// ============================================================================
// Server Push Types (server-initiated, no request id)
// ============================================================================

/// Server-initiated push message (no request id).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerPush {
	Change(ChangePayload),
}

/// Payload for subscription change notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangePayload {
	pub subscription_id: String,
	/// The frame containing change data.
	pub frame: WebsocketFrame,
}
