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
	Err(ErrorResponse),
	Command(CommandResponse),
	Query(QueryResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
	pub diagnostic: Diagnostic,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResponse {
	pub frames: Vec<ResponseFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
	pub frames: Vec<ResponseFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseFrame {
	pub row_numbers: Vec<u64>,
	pub columns: Vec<ResponseColumn>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseColumn {
	pub namespace: Option<String>,
	pub store: Option<String>,
	pub name: String,
	pub r#type: Type,
	pub data: Vec<String>,
}
