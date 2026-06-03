// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_auth::service::{AuthResponse, AuthService};
use reifydb_core::{
	actors::server::{
		ServerAuthResponse, ServerLogoutResponse, ServerMessage, ServerResponse, ServerSubscribeResponse,
	},
	execution::ExecutionResult,
};
use reifydb_engine::{engine::StandardEngine, session::RetryStrategy};
use reifydb_runtime::{
	actor::{
		context::Context,
		reply::Reply,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_value::{
	params::Params,
	value::{duration::Duration, identity::IdentityId},
};

pub struct ServerActor {
	engine: StandardEngine,
	auth_service: AuthService,
	clock: Clock,
	retry: RetryStrategy,
}

impl ServerActor {
	pub fn new(engine: StandardEngine, auth_service: AuthService, clock: Clock) -> Self {
		Self {
			engine,
			auth_service,
			clock,
			retry: RetryStrategy::default(),
		}
	}

	fn dispatch_execute(
		&self,
		identity: IdentityId,
		rql: String,
		params: Params,
		reply: Reply<ServerResponse>,
		execute: impl Fn(&StandardEngine, IdentityId, &str, Params) -> ExecutionResult,
	) {
		let t = self.clock.instant();
		let result = self
			.retry
			.execute(self.engine.rng(), &rql, || execute(&self.engine, identity, &rql, params.clone()));
		if let Some(err) = result.error {
			reply.send(ServerResponse::EngineError {
				diagnostic: Box::new(err.diagnostic()),
				rql,
			});
		} else {
			reply.send(ServerResponse::Success {
				frames: result.frames,
				duration: Duration::from_std(t.elapsed()),
				metrics: result.metrics,
			});
		}
	}

	#[inline]
	fn handle_subscribe(&self, identity: IdentityId, rql: String, reply: Reply<ServerSubscribeResponse>) {
		let t = self.clock.instant();
		let result = self.engine.subscribe_as(identity, &rql, Params::None);
		if let Some(err) = result.error {
			reply.send(ServerSubscribeResponse::EngineError {
				diagnostic: Box::new(err.diagnostic()),
				rql,
			});
		} else {
			reply.send(ServerSubscribeResponse::Subscribed {
				frames: result.frames,
				duration: Duration::from_std(t.elapsed()),
				metrics: result.metrics,
			});
		}
	}

	#[inline]
	fn handle_authenticate(
		&self,
		method: String,
		credentials: HashMap<String, String>,
		reply: Reply<ServerAuthResponse>,
	) {
		match self.auth_service.authenticate(&method, credentials) {
			Ok(AuthResponse::Authenticated {
				identity,
				token,
			}) => reply.send(ServerAuthResponse::Authenticated {
				identity,
				token,
			}),
			Ok(AuthResponse::Challenge {
				challenge_id,
				payload,
			}) => reply.send(ServerAuthResponse::Challenge {
				challenge_id,
				payload,
			}),
			Ok(AuthResponse::Failed {
				reason,
			}) => reply.send(ServerAuthResponse::Failed {
				reason,
			}),
			Err(e) => reply.send(ServerAuthResponse::Error(e.to_string())),
		}
	}

	#[inline]
	fn handle_logout(&self, token: String, reply: Reply<ServerLogoutResponse>) {
		if self.auth_service.revoke_token(&token) {
			reply.send(ServerLogoutResponse::Ok);
		} else {
			reply.send(ServerLogoutResponse::InvalidToken);
		}
	}
}

impl Actor for ServerActor {
	type State = ();
	type Message = ServerMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut (), msg: ServerMessage, _ctx: &Context<ServerMessage>) -> Directive {
		match msg {
			ServerMessage::Query {
				identity,
				rql,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, rql, params, reply, |e, id, s, p| e.query_as(id, s, p));
			}
			ServerMessage::Command {
				identity,
				rql,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, rql, params, reply, |e, id, s, p| {
					e.command_as(id, s, p)
				});
			}
			ServerMessage::Admin {
				identity,
				rql,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, rql, params, reply, |e, id, s, p| e.admin_as(id, s, p));
			}
			ServerMessage::Subscribe {
				identity,
				rql,
				reply,
			} => {
				self.handle_subscribe(identity, rql, reply);
			}
			ServerMessage::Authenticate {
				method,
				credentials,
				reply,
			} => {
				self.handle_authenticate(method, credentials, reply);
			}
			ServerMessage::Logout {
				token,
				reply,
			} => {
				self.handle_logout(token, reply);
			}
		}
		Directive::Continue
	}
}
