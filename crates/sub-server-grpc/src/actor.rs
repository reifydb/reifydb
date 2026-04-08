// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Actor implementation for the gRPC server.
//!
//! Handles engine dispatch (query, command, admin, subscribe) and auth operations.
//! Same `handle()` code runs in both native and DST modes.

use reifydb_auth::service::{AuthResponse, AuthService};
use reifydb_core::actors::{
	grpc::GrpcMessage,
	server::{ServerAuthResponse, ServerLogoutResponse, ServerResponse, ServerSubscribeResponse},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_sub_server::subscribe::extract_subscription_id;
use reifydb_type::params::Params;

pub struct GrpcServerActor {
	engine: StandardEngine,
	auth_service: AuthService,
	clock: Clock,
}

impl GrpcServerActor {
	pub fn new(engine: StandardEngine, auth_service: AuthService, clock: Clock) -> Self {
		Self {
			engine,
			auth_service,
			clock,
		}
	}
}

impl Actor for GrpcServerActor {
	type State = ();
	type Message = GrpcMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut (), msg: GrpcMessage, _ctx: &Context<GrpcMessage>) -> Directive {
		match msg {
			GrpcMessage::Query {
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
			GrpcMessage::Command {
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
			GrpcMessage::Admin {
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
			GrpcMessage::Subscribe {
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
					let subscription_id = extract_subscription_id(&result.frames)
						.expect("subscribe_as must return subscription_id in result frame");
					reply.send(ServerSubscribeResponse::Subscribed {
						subscription_id,
						frames: result.frames,
						duration: t.elapsed(),
					});
				}
			}
			GrpcMessage::Authenticate {
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
			GrpcMessage::Logout {
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
