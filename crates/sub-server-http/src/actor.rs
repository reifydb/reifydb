// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor implementation for the HTTP server.
//!
//! The actor handles engine dispatch (query, command, admin) and auth operations.
//! The same `handle()` code runs in both native (rayon pool) and DST modes.

use reifydb_auth::service::{AuthResponse, AuthService};
use reifydb_core::actors::{
	http::HttpMessage,
	server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
pub struct HttpServerActor {
	engine: StandardEngine,
	auth_service: AuthService,
	clock: Clock,
}

impl HttpServerActor {
	pub fn new(engine: StandardEngine, auth_service: AuthService, clock: Clock) -> Self {
		Self {
			engine,
			auth_service,
			clock,
		}
	}
}

impl Actor for HttpServerActor {
	type State = ();
	type Message = HttpMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut (), msg: HttpMessage, _ctx: &Context<HttpMessage>) -> Directive {
		match msg {
			HttpMessage::Query {
				identity,
				statements,
				params,
				reply,
			} => {
				let combined = statements.join("; ");
				let t = self.clock.instant();
				let result = self.engine.query_as(identity, &combined, params);
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
			HttpMessage::Command {
				identity,
				statements,
				params,
				reply,
			} => {
				let combined = statements.join("; ");
				let t = self.clock.instant();
				let result = self.engine.command_as(identity, &combined, params);
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
			HttpMessage::Admin {
				identity,
				statements,
				params,
				reply,
			} => {
				let combined = statements.join("; ");
				let t = self.clock.instant();
				let result = self.engine.admin_as(identity, &combined, params);
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
			HttpMessage::Authenticate {
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
			HttpMessage::Logout {
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
