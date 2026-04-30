// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_sub_server::{format::WireFormat, wire::WireParams};
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
/// - `"BatchSubscribe"` - Subscribe to N queries in a single batch
/// - `"BatchUnsubscribe"` - Unsubscribe a batch (cascades to all members)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum RequestPayload {
	Auth(AuthRequest),
	Admin(AdminRequest),
	Command(CommandRequest),
	Query(QueryRequest),
	Subscribe(SubscribeRequest),
	Unsubscribe(UnsubscribeRequest),
	BatchSubscribe(BatchSubscribeRequest),
	BatchUnsubscribe(BatchUnsubscribeRequest),
	Call(CallRequest),
	Logout,
}

/// Admin (DDL + DML + Query) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct AdminRequest {
	/// RQL string to execute.
	pub rql: String,
	/// Optional parameters for the statements.
	pub params: Option<WireParams>,
	/// Response format. Defaults to `Frames` when absent.
	#[serde(default)]
	pub format: WireFormat,
	/// When true with format="json", return the first element directly instead of an array.
	pub unwrap: Option<bool>,
}

/// Authentication request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
	pub token: Option<String>,
	pub method: Option<String>,
	pub credentials: Option<HashMap<String, String>>,
}

/// Command (write) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandRequest {
	/// RQL string to execute.
	pub rql: String,
	/// Optional parameters for the statements.
	pub params: Option<WireParams>,
	/// Response format. Defaults to `Frames` when absent.
	#[serde(default)]
	pub format: WireFormat,
	/// When true with format="json", return the first element directly instead of an array.
	pub unwrap: Option<bool>,
}

/// Query (read) request payload.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
	/// RQL string to execute.
	pub rql: String,
	/// Optional parameters for the queries.
	pub params: Option<WireParams>,
	/// Response format. Defaults to `Frames` when absent.
	#[serde(default)]
	pub format: WireFormat,
	/// When true with format="json", return the first element directly instead of an array.
	pub unwrap: Option<bool>,
}

/// Subscribe request payload.
///
/// Subscribes to real-time changes from a query. The server will push
/// Change messages whenever the query results change.
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeRequest {
	/// RQL query to subscribe to.
	pub rql: String,
	/// Wire format for pushed changes. Defaults to `Frames` when absent.
	#[serde(default)]
	pub format: WireFormat,
}

/// Unsubscribe request payload.
///
/// Stops receiving changes for a previously created subscription.
#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeRequest {
	/// The subscription ID returned from a Subscribe response.
	pub subscription_id: String,
}

/// Batch-subscribe request payload.
///
/// Subscribes to multiple RQL queries as a single batch. The server coalesces
/// deltas from all members that arrive within one poller tick into one push.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchSubscribeRequest {
	/// RQL queries to subscribe to. Each becomes a member subscription.
	pub queries: Vec<String>,
	/// Wire format for pushed changes. Applies to every member. Defaults to `Frames`.
	#[serde(default)]
	pub format: WireFormat,
}

/// Batch-unsubscribe request payload.
///
/// Cascade-cancels every member of the batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchUnsubscribeRequest {
	/// Batch ID returned from a BatchSubscribe response.
	pub batch_id: String,
}

/// Call request payload.
///
/// Invokes a bound procedure by its WS binding name.
#[derive(Debug, Serialize, Deserialize)]
pub struct CallRequest {
	/// Globally unique WS binding name.
	pub name: String,
	/// Named parameters to pass to the procedure.
	pub params: Option<WireParams>,
}
