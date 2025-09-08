// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod client;
mod domain;
pub mod http;
pub mod session;
mod utils;
pub mod ws;

pub use client::Client;
pub use domain::{Frame, FrameColumn};
pub use http::{
	HttpBlockingSession, HttpCallbackSession, HttpChannelSession,
	HttpClient, HttpResponseMessage,
};
use reifydb_type::diagnostic::Diagnostic;
// Re-export types from reifydb
pub use reifydb_type::{OrderedF32, OrderedF64, Params, Type, Value};
use serde::{Deserialize, Serialize};
pub use session::{CommandResult, QueryResult};
pub use ws::{
	ChannelResponse, ResponseMessage, WsBlockingSession, WsCallbackSession,
	WsChannelSession, client::WsClient,
};

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
	pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub schema: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}
