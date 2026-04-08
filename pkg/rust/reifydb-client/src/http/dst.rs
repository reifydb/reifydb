// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! DST client for the HTTP server actor.
//!
//! Provides a synchronous client that sends messages to the HttpServerActor
//! and reads replies after `run_until_idle()`.

use std::collections::HashMap;

use reifydb_core::actors::{
	http::HttpMessage,
	server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse},
};
use reifydb_runtime::actor::{mailbox::ActorRef, reply::reply_channel, system::ActorSystem};
use reifydb_type::{params::Params, value::identity::IdentityId};

/// Synchronous DST client for the HTTP server actor.
pub struct DstHttpClient {
	actor_ref: ActorRef<HttpMessage>,
	system: ActorSystem,
}

impl DstHttpClient {
	pub fn new(actor_ref: ActorRef<HttpMessage>, system: ActorSystem) -> Self {
		Self {
			actor_ref,
			system,
		}
	}

	fn send(&self, msg: HttpMessage) {
		self.actor_ref.send(msg).ok().expect("actor mailbox closed");
		self.system.run_until_idle();
	}

	pub fn query(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(HttpMessage::Query {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn command(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(HttpMessage::Command {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn admin(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(HttpMessage::Admin {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> ServerAuthResponse {
		let (reply, receiver) = reply_channel();
		self.send(HttpMessage::Authenticate {
			method,
			credentials,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn logout(&self, token: String) -> ServerLogoutResponse {
		let (reply, receiver) = reply_channel();
		self.send(HttpMessage::Logout {
			token,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}
}
