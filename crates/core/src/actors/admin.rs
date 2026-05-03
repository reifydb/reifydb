// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};

pub type AdminHandle = ActorHandle<AdminMessage>;

pub enum AdminMessage {
	Execute {
		query: String,
		reply: Reply<AdminExecuteResponse>,
	},

	Login {
		token: String,
		reply: Reply<AdminLoginResponse>,
	},

	Logout {
		reply: Reply<AdminLogoutResponse>,
	},

	AuthStatus {
		reply: Reply<AdminAuthStatusResponse>,
	},
}

pub enum AdminExecuteResponse {
	Success {
		message: String,
	},
	NotImplemented,
	Error(String),
}

pub enum AdminLoginResponse {
	Success {
		session_token: String,
	},
	AuthNotRequired,
	InvalidToken,
}

pub enum AdminLogoutResponse {
	Ok,
}

pub struct AdminAuthStatusResponse {
	pub auth_required: bool,
	pub authenticated: bool,
}
