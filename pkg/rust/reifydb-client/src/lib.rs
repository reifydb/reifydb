// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

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
use reifydb_type::diagnostic::Diagnostic;
pub use reifydb_type::{
	Frame, FrameColumn, FrameColumnData, FrameError, FrameRow, FrameRows, FromFrameError, FromValueError,
	OrderedF32, OrderedF64, Params, TryFromValue, TryFromValueCoerce, Type, Value,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub namespace: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}
