// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_auth::service::{AuthResponse, AuthService};
use reifydb_core::{
	actors::server::{
		ServerAuthResponse, ServerLogoutResponse, ServerMessage, ServerResponse, ServerSubscribeResponse,
	},
	execution::ExecutionResult,
	interface::catalog::procedure::Procedure,
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
	error::Diagnostic,
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
	fn handle_call(&self, identity: IdentityId, name: String, params: Params, reply: Reply<ServerResponse>) {
		let catalog = self.engine.catalog();
		let cache = catalog.cache();
		let binding = match cache.find_ws_binding_by_name(&name) {
			Some(binding) => binding,
			None => {
				return reply.send(call_error(
					"NOT_FOUND",
					format!("no WS binding named `{}`", name),
					name,
				));
			}
		};
		let procedure = match cache.find_procedure(binding.procedure_id) {
			Some(procedure) => procedure,
			None => {
				return reply.send(call_error(
					"INTERNAL_ERROR",
					"binding references missing procedure".to_string(),
					name,
				));
			}
		};
		let namespace = match cache.find_namespace(binding.namespace) {
			Some(namespace) => namespace,
			None => {
				return reply.send(call_error(
					"INTERNAL_ERROR",
					"binding references missing namespace".to_string(),
					name,
				));
			}
		};
		if let Err(diagnostic) = validate_call_params(&procedure, &params) {
			return reply.send(ServerResponse::EngineError {
				diagnostic,
				rql: name,
			});
		}
		let rql = format!("CALL {}::{}()", namespace.name(), procedure.name());
		self.dispatch_execute(identity, rql, params, reply, |e, id, s, p| e.command_as(id, s, p));
	}

	#[inline]
	fn handle_subscribe(
		&self,
		identity: IdentityId,
		rql: String,
		params: Params,
		reply: Reply<ServerSubscribeResponse>,
	) {
		let t = self.clock.instant();
		let result = self.engine.subscribe_as(identity, &rql, params);
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
			ServerMessage::Call {
				identity,
				name,
				params,
				reply,
			} => {
				self.handle_call(identity, name, params, reply);
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
				params,
				reply,
			} => {
				self.handle_subscribe(identity, rql, params, reply);
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

fn call_error(code: &str, message: String, rql: String) -> ServerResponse {
	ServerResponse::EngineError {
		diagnostic: call_diagnostic(code, message),
		rql,
	}
}

fn call_diagnostic(code: &str, message: String) -> Box<Diagnostic> {
	Box::new(Diagnostic {
		code: code.to_string(),
		message,
		..Default::default()
	})
}

fn validate_call_params(procedure: &Procedure, params: &Params) -> Result<(), Box<Diagnostic>> {
	match params {
		Params::None => {
			if let Some(p) = procedure.params().first() {
				return Err(call_diagnostic(
					"INVALID_PARAMS",
					format!("missing required parameter `{}`", p.name),
				));
			}
		}
		Params::Named(map) => {
			for key in map.keys() {
				if !procedure.params().iter().any(|p| &p.name == key) {
					return Err(call_diagnostic(
						"INVALID_PARAMS",
						format!("unknown parameter `{}`", key),
					));
				}
			}
			for p in procedure.params() {
				if !map.contains_key(&p.name) {
					return Err(call_diagnostic(
						"INVALID_PARAMS",
						format!("missing required parameter `{}`", p.name),
					));
				}
			}
		}
		Params::Positional(_) => {
			return Err(call_diagnostic("INVALID_PARAMS", "Call requires named params".to_string()));
		}
	}
	Ok(())
}
