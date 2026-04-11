// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared message and response types for network server actors.
//!
//! `ServerMessage` is the unified message type used by HTTP, gRPC, and WebSocket
//! server actors. The same actor handles all protocols — the transport layer is
//! just a thin shell that converts protocol-specific requests into `ServerMessage`.

use std::{collections::HashMap, time::Duration};

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::{
	error::Diagnostic,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::interface::catalog::id::SubscriptionId;

/// The type of database operation being executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
	Query,
	Command,
	Admin,
	Subscribe,
}

/// Handle to a server actor.
pub type ServerHandle = ActorHandle<ServerMessage>;

/// Unified message type for all network server actors (HTTP, gRPC, WebSocket).
pub enum ServerMessage {
	/// Execute a read-only query.
	Query {
		identity: IdentityId,
		statements: Vec<String>,
		params: Params,
		reply: Reply<ServerResponse>,
	},
	/// Execute a write command.
	Command {
		identity: IdentityId,
		statements: Vec<String>,
		params: Params,
		reply: Reply<ServerResponse>,
	},
	/// Execute an admin operation.
	Admin {
		identity: IdentityId,
		statements: Vec<String>,
		params: Params,
		reply: Reply<ServerResponse>,
	},
	/// Create a subscription.
	Subscribe {
		identity: IdentityId,
		query: String,
		reply: Reply<ServerSubscribeResponse>,
	},
	/// Authenticate with credentials.
	Authenticate {
		method: String,
		credentials: HashMap<String, String>,
		reply: Reply<ServerAuthResponse>,
	},
	/// Logout / revoke a session token.
	Logout {
		token: String,
		reply: Reply<ServerLogoutResponse>,
	},
}

/// Response from an engine dispatch operation (query, command, admin).
pub enum ServerResponse {
	/// Operation succeeded with result frames and compute duration.
	Success {
		frames: Vec<Frame>,
		duration: Duration,
	},
	/// Engine returned an error.
	EngineError {
		diagnostic: Box<Diagnostic>,
		statement: String,
	},
}

/// Response from an authentication attempt.
pub enum ServerAuthResponse {
	/// Authentication succeeded.
	Authenticated {
		identity: IdentityId,
		token: String,
	},
	/// Challenge-response round-trip required.
	Challenge {
		challenge_id: String,
		payload: HashMap<String, String>,
	},
	/// Authentication failed.
	Failed {
		reason: String,
	},
	/// Internal error during authentication.
	Error(String),
}

/// Response from a logout attempt.
pub enum ServerLogoutResponse {
	/// Token successfully revoked.
	Ok,
	/// Token was invalid or already expired.
	InvalidToken,
	/// Internal error during logout.
	Error(String),
}

/// Response from a subscribe operation.
pub enum ServerSubscribeResponse {
	/// Subscription created successfully.
	Subscribed {
		subscription_id: SubscriptionId,
		frames: Vec<Frame>,
		duration: Duration,
	},
	/// Engine returned an error.
	EngineError {
		diagnostic: Box<Diagnostic>,
		statement: String,
	},
}

/// Build the appropriate `ServerMessage` from operation parameters.
///
/// Used by both the native `dispatch()` function and DST clients to construct
/// messages for the `ServerActor`.
pub fn build_server_message(
	operation: Operation,
	identity: IdentityId,
	statements: Vec<String>,
	params: Params,
	reply: Reply<ServerResponse>,
) -> ServerMessage {
	match operation {
		Operation::Query => ServerMessage::Query {
			identity,
			statements,
			params,
			reply,
		},
		Operation::Command => ServerMessage::Command {
			identity,
			statements,
			params,
			reply,
		},
		Operation::Admin => ServerMessage::Admin {
			identity,
			statements,
			params,
			reply,
		},
		Operation::Subscribe => unreachable!("subscribe uses a different dispatch path"),
	}
}
