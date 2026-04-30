// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::actors::admin::{
	AdminAuthStatusResponse, AdminExecuteResponse, AdminLoginResponse, AdminLogoutResponse, AdminMessage,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};

pub struct AdminServerActor {
	engine: StandardEngine,
	auth_required: bool,
	auth_token: Option<String>,
	clock: Clock,
}

impl AdminServerActor {
	pub fn new(engine: StandardEngine, auth_required: bool, auth_token: Option<String>, clock: Clock) -> Self {
		Self {
			engine,
			auth_required,
			auth_token,
			clock,
		}
	}
}

impl Actor for AdminServerActor {
	type State = ();
	type Message = AdminMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut (), msg: AdminMessage, _ctx: &Context<AdminMessage>) -> Directive {
		match msg {
			AdminMessage::Execute {
				reply,
				..
			} => {
				// TODO: Execute query using the engine
				let _ = &self.engine;
				let _ = &self.clock;
				reply.send(AdminExecuteResponse::NotImplemented);
			}
			AdminMessage::Login {
				token,
				reply,
			} => {
				if !self.auth_required {
					reply.send(AdminLoginResponse::AuthNotRequired);
				} else if self.auth_token.as_deref() == Some(&token) {
					// TODO: Generate proper session token
					reply.send(AdminLoginResponse::Success {
						session_token: "temp_session_token".to_string(),
					});
				} else {
					reply.send(AdminLoginResponse::InvalidToken);
				}
			}
			AdminMessage::Logout {
				reply,
			} => {
				reply.send(AdminLogoutResponse::Ok);
			}
			AdminMessage::AuthStatus {
				reply,
			} => {
				reply.send(AdminAuthStatusResponse {
					auth_required: self.auth_required,
					// TODO: Check actual auth status from session
					authenticated: !self.auth_required,
				});
			}
		}
		Directive::Continue
	}
}
