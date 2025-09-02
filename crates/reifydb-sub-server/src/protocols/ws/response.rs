// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{Type, diagnostic::Diagnostic};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct WebsocketFrame {
	pub columns: Vec<WebsocketColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WebsocketColumn {
	pub schema: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}
