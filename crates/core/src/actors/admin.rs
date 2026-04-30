// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};

/// Handle to the admin server actor.
pub type AdminHandle = ActorHandle<AdminMessage>;

/// Messages for the admin server actor.
pub enum AdminMessage {
	/// Execute a query.
	Execute {
		query: String,
		reply: Reply<AdminExecuteResponse>,
	},
	/// Login with a token.
	Login {
		token: String,
		reply: Reply<AdminLoginResponse>,
	},
	/// Logout.
	Logout {
		reply: Reply<AdminLogoutResponse>,
	},
	/// Check auth status.
	AuthStatus {
		reply: Reply<AdminAuthStatusResponse>,
	},
}

/// Response to an execute request.
pub enum AdminExecuteResponse {
	Success {
		message: String,
	},
	NotImplemented,
	Error(String),
}

/// Response to a login request.
pub enum AdminLoginResponse {
	Success {
		session_token: String,
	},
	AuthNotRequired,
	InvalidToken,
}

/// Response to a logout request.
pub enum AdminLogoutResponse {
	Ok,
}

/// Response to an auth status request.
pub struct AdminAuthStatusResponse {
	pub auth_required: bool,
	pub authenticated: bool,
}
