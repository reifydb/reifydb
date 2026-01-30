// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WebSocket request types for the protocol layer.
//!
//! These types define the JSON message format for WebSocket client-server communication.

use reifydb_type::params::Params;
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
/// - `"Admin"` - Admin operation (DDL + DML + Query)
/// - `"Command"` - Write command (INSERT, UPDATE, DELETE)
/// - `"Query"` - Read query (SELECT)
/// - `"Subscribe"` - Subscribe to real-time changes
/// - `"Unsubscribe"` - Unsubscribe from a subscription
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

/// Admin (DDL + DML + Query) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	/// RQL statements to execute.
	pub statements: Vec<String>,
	/// Optional parameters for the statements.
	pub params: Option<Params>,
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

/// Subscribe request payload.
///
/// Subscribes to real-time changes from a query. The server will push
/// Change messages whenever the query results change.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	/// RQL query to subscribe to.
	pub query: String,
}

/// Unsubscribe request payload.
///
/// Stops receiving changes for a previously created subscription.
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	/// The subscription ID returned from a Subscribe response.
	pub subscription_id: String,
}
