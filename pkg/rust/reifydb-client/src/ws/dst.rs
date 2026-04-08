// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! DST client for the WebSocket server actor.

use std::collections::HashMap;

use reifydb_core::actors::{
	server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse, ServerSubscribeResponse},
	ws::WsMessage,
};
use reifydb_runtime::actor::{mailbox::ActorRef, reply::reply_channel, system::ActorSystem};
use reifydb_type::{params::Params, value::identity::IdentityId};

/// Synchronous DST client for a single WebSocket connection actor.
pub struct DstWsClient {
	actor_ref: ActorRef<WsMessage>,
	system: ActorSystem,
}

impl DstWsClient {
	pub fn new(actor_ref: ActorRef<WsMessage>, system: ActorSystem) -> Self {
		Self {
			actor_ref,
			system,
		}
	}

	fn send(&self, msg: WsMessage) {
		self.actor_ref.send(msg).ok().expect("actor mailbox closed");
		self.system.run_until_idle();
	}

	pub fn query(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Query {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn command(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Command {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn admin(&self, identity: IdentityId, statements: Vec<String>, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Admin {
			identity,
			statements,
			params,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn subscribe(&self, identity: IdentityId, query: String) -> ServerSubscribeResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Subscribe {
			identity,
			query,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> ServerAuthResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Authenticate {
			method,
			credentials,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn logout(&self, token: String) -> ServerLogoutResponse {
		let (reply, receiver) = reply_channel();
		self.send(WsMessage::Logout {
			token,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}
}
