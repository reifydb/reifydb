// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor messages for the HTTP server.

use std::collections::HashMap;

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::{params::Params, value::identity::IdentityId};

use super::server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse};

/// Handle to the HTTP server actor.
pub type HttpHandle = ActorHandle<HttpMessage>;

/// Messages for the HTTP server actor.
pub enum HttpMessage {
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
