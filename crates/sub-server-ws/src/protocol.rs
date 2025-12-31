// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

//! WebSocket request types for the protocol layer.
//!
//! These types define the JSON message format for WebSocket client-server communication.

use reifydb_type::Params;
use serde::{Deserialize, Serialize};

/// A WebSocket request message.
///
/// Each request has a unique `id` that clients use to correlate responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
	pub id: String,
	#[serde(flatten)]
	pub payload: RequestPayload,
}

/// The payload of a WebSocket request.
///
/// Discriminated by the `type` field in JSON:
/// - `"Auth"` - Authentication request
/// - `"Command"` - Write command (INSERT, UPDATE, DELETE, DDL)
/// - `"Query"` - Read query (SELECT)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Command(CommandRequest),
	Query(QueryRequest),
}

/// Authentication request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	/// Bearer token for authentication.
	pub token: Option<String>,
}

/// Command (write) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	/// RQL statements to execute.
	pub statements: Vec<String>,
	/// Optional parameters for the statements.
	pub params: Option<Params>,
}

/// Query (read) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	/// RQL query statements to execute.
	pub statements: Vec<String>,
	/// Optional parameters for the queries.
	pub params: Option<Params>,
}
