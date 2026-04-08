// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor messages for the WebSocket server.

use std::collections::HashMap;

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::{params::Params, value::identity::IdentityId};

use super::server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse, ServerSubscribeResponse};

/// Handle to the WebSocket server actor.
pub type WsHandle = ActorHandle<WsMessage>;

/// Messages for the WebSocket server actor.
pub enum WsMessage {
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
