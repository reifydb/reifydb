// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified server actor for HTTP, gRPC, and WebSocket transports.
//!
//! The same `handle()` code runs in both native (rayon pool) and DST modes.
//! Protocol-specific concerns (serialization, HTTP status codes, etc.) live in
//! the transport layer — this actor only does engine dispatch and auth.

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
use reifydb_type::{params::Params, value::identity::IdentityId};

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
		statements: Vec<String>,
		params: Params,
		reply: Reply<ServerResponse>,
		execute: impl Fn(&StandardEngine, IdentityId, &str, Params) -> ExecutionResult,
	) {
		let combined = statements.join("; ");
		let t = self.clock.instant();
		let result = self.retry.execute(self.engine.rng(), &combined, || {
			execute(&self.engine, identity, &combined, params.clone())
		});
		if let Some(err) = result.error {
			reply.send(ServerResponse::EngineError {
				diagnostic: Box::new(err.diagnostic()),
				statement: combined,
			});
		} else {
			reply.send(ServerResponse::Success {
				frames: result.frames,
				duration: t.elapsed(),
			});
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
				statements,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, statements, params, reply, |e, id, s, p| {
					e.query_as(id, s, p)
				});
			}
			ServerMessage::Command {
				identity,
				statements,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, statements, params, reply, |e, id, s, p| {
					e.command_as(id, s, p)
				});
			}
			ServerMessage::Admin {
				identity,
				statements,
				params,
				reply,
			} => {
				self.dispatch_execute(identity, statements, params, reply, |e, id, s, p| {
					e.admin_as(id, s, p)
				});
			}
			ServerMessage::Subscribe {
				identity,
				query,
				reply,
			} => {
				let t = self.clock.instant();
				let result = self.engine.subscribe_as(identity, &query, Params::None);
				if let Some(err) = result.error {
					reply.send(ServerSubscribeResponse::EngineError {
						diagnostic: Box::new(err.diagnostic()),
						statement: query,
					});
				} else {
					reply.send(ServerSubscribeResponse::Subscribed {
						frames: result.frames,
						duration: t.elapsed(),
					});
				}
			}
			ServerMessage::Authenticate {
				method,
				credentials,
				reply,
			} => match self.auth_service.authenticate(&method, credentials) {
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
			},
			ServerMessage::Logout {
				token,
				reply,
			} => {
				if self.auth_service.revoke_token(&token) {
					reply.send(ServerLogoutResponse::Ok);
				} else {
					reply.send(ServerLogoutResponse::InvalidToken);
				}
			}
		}
		Directive::Continue
	}
}
