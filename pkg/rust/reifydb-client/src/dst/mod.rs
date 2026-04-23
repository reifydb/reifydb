// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! DST (deterministic simulation testing) client for the server actor.
//!
//! Provides a synchronous client that sends `ServerMessage`s to the unified
//! `ServerActor` and reads replies after `run_until_idle()`. There is no
//! protocol distinction in DST - all operations go through the same actor.

use std::collections::HashMap;

use reifydb_core::actors::server::{
	Operation, ServerAuthResponse, ServerLogoutResponse, ServerMessage, ServerResponse, ServerSubscribeResponse,
	build_server_message,
};
use reifydb_runtime::actor::{mailbox::ActorRef, reply::reply_channel, system::ActorSystem};
use reifydb_type::{params::Params, value::identity::IdentityId};

pub struct DstClient {
	actor_ref: ActorRef<ServerMessage>,
	system: ActorSystem,
}

impl DstClient {
	pub fn new(actor_ref: ActorRef<ServerMessage>, system: ActorSystem) -> Self {
		Self {
			actor_ref,
			system,
		}
	}

	fn send(&self, msg: ServerMessage) {
		self.actor_ref.send(msg).ok().expect("actor mailbox closed");
		self.system.run_until_idle();
	}

	pub fn query(&self, identity: IdentityId, rql: String, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(build_server_message(Operation::Query, identity, rql, params, reply));
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn command(&self, identity: IdentityId, rql: String, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(build_server_message(Operation::Command, identity, rql, params, reply));
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn admin(&self, identity: IdentityId, rql: String, params: Params) -> ServerResponse {
		let (reply, receiver) = reply_channel();
		self.send(build_server_message(Operation::Admin, identity, rql, params, reply));
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn subscribe(&self, identity: IdentityId, rql: String) -> ServerSubscribeResponse {
		let (reply, receiver) = reply_channel();
		self.send(ServerMessage::Subscribe {
			identity,
			rql,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn authenticate(&self, method: String, credentials: HashMap<String, String>) -> ServerAuthResponse {
		let (reply, receiver) = reply_channel();
		self.send(ServerMessage::Authenticate {
			method,
			credentials,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}

	pub fn logout(&self, token: String) -> ServerLogoutResponse {
		let (reply, receiver) = reply_channel();
		self.send(ServerMessage::Logout {
			token,
			reply,
		});
		receiver.try_recv().expect("no reply from actor")
	}
}
