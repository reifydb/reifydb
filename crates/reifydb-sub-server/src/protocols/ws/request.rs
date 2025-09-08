// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::Params;
use serde::{Deserialize, Serialize};

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
