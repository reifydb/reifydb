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
use reifydb_type::error::diagnostic::Diagnostic;
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
pub use session::{CommandResult, QueryResult};
pub use ws::WsClient;

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
	Command(CommandRequest),
	Query(QueryRequest),
	Subscribe(SubscribeRequest),
	Unsubscribe(UnsubscribeRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	pub statements: Vec<String>,
	pub params: Option<Params>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	pub statements: Vec<String>,
	pub params: Option<Params>,
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
	Command(CommandResponse),
	Query(QueryResponse),
	Subscribed(SubscribedResponse),
	Unsubscribed(UnsubscribedResponse),
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
