// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, time::Duration};

use reifydb_runtime::actor::{reply::Reply, system::ActorHandle};
use reifydb_type::{
	error::Diagnostic,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::metric::ExecutionMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
	Query,
	Command,
	Admin,
	Subscribe,
}

pub type ServerHandle = ActorHandle<ServerMessage>;

pub enum ServerMessage {
	Query {
		identity: IdentityId,
		rql: String,
		params: Params,
		reply: Reply<ServerResponse>,
	},

	Command {
		identity: IdentityId,
		rql: String,
		params: Params,
		reply: Reply<ServerResponse>,
	},

	Admin {
		identity: IdentityId,
		rql: String,
		params: Params,
		reply: Reply<ServerResponse>,
	},

	Subscribe {
		identity: IdentityId,
		rql: String,
		reply: Reply<ServerSubscribeResponse>,
	},

	Authenticate {
		method: String,
		credentials: HashMap<String, String>,
		reply: Reply<ServerAuthResponse>,
	},

	Logout {
		token: String,
		reply: Reply<ServerLogoutResponse>,
	},
}

pub enum ServerResponse {
	Success {
		frames: Vec<Frame>,
		duration: Duration,
		metrics: ExecutionMetrics,
	},

	EngineError {
		diagnostic: Box<Diagnostic>,
		rql: String,
	},
}

pub enum ServerAuthResponse {
	Authenticated {
		identity: IdentityId,
		token: String,
	},

	Challenge {
		challenge_id: String,
		payload: HashMap<String, String>,
	},

	Failed {
		reason: String,
	},

	Error(String),
}

pub enum ServerLogoutResponse {
	Ok,

	InvalidToken,

	Error(String),
}

pub enum ServerSubscribeResponse {
	Subscribed {
		frames: Vec<Frame>,
		duration: Duration,
		metrics: ExecutionMetrics,
	},

	EngineError {
		diagnostic: Box<Diagnostic>,
		rql: String,
	},
}

pub fn build_server_message(
	operation: Operation,
	identity: IdentityId,
	rql: String,
	params: Params,
	reply: Reply<ServerResponse>,
) -> ServerMessage {
	match operation {
		Operation::Query => ServerMessage::Query {
			identity,
			rql,
			params,
			reply,
		},
		Operation::Command => ServerMessage::Command {
			identity,
			rql,
			params,
			reply,
		},
		Operation::Admin => ServerMessage::Admin {
			identity,
			rql,
			params,
			reply,
		},
		Operation::Subscribe => unreachable!("subscribe uses a different dispatch path"),
	}
}
