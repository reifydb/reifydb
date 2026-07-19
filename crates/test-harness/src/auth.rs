// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_auth::{
	github::GithubApi,
	registry::AuthenticationRegistry,
	service::{AuthConfigurator, AuthEngine, AuthResponse, AuthService},
};
use reifydb_runtime::context::{clock::Clock, rng::Rng};
use reifydb_value::value::identity::IdentityId;

use crate::engine::AsEngine;

pub fn auth_service(engine: &impl AsEngine) -> AuthServiceFactory {
	AuthServiceFactory {
		engine: Arc::new(engine.standard_engine().clone()),
		configurator: AuthConfigurator::new(),
		github_api: None,
	}
}

pub struct AuthServiceFactory {
	engine: Arc<dyn AuthEngine>,
	configurator: AuthConfigurator,
	github_api: Option<Arc<dyn GithubApi>>,
}

impl AuthServiceFactory {
	pub fn configure(mut self, configure: impl FnOnce(AuthConfigurator) -> AuthConfigurator) -> Self {
		self.configurator = configure(self.configurator);
		self
	}

	pub fn github_api(mut self, api: Arc<dyn GithubApi>) -> Self {
		self.github_api = Some(api);
		self
	}

	pub fn build(self) -> AuthService {
		let registry = Arc::new(AuthenticationRegistry::default());
		let rng = Rng::seeded(42);
		let clock = Clock::Real;
		let config = self.configurator.configure();
		match self.github_api {
			Some(api) => AuthService::with_github_api(self.engine, registry, rng, clock, config, api),
			None => AuthService::new(self.engine, registry, rng, clock, config),
		}
	}
}

pub trait AuthResponseAssert {
	fn expect_authenticated(self) -> (IdentityId, String);
	fn expect_challenge(self) -> (String, HashMap<String, String>);
	fn expect_failed(self, reason: &str);
}

impl AuthResponseAssert for AuthResponse {
	fn expect_authenticated(self) -> (IdentityId, String) {
		match self {
			AuthResponse::Authenticated {
				identity,
				token,
			} => (identity, token),
			other => panic!("expected authenticated response, got {other:?}"),
		}
	}

	fn expect_challenge(self) -> (String, HashMap<String, String>) {
		match self {
			AuthResponse::Challenge {
				challenge_id,
				payload,
			} => (challenge_id, payload),
			other => panic!("expected challenge response, got {other:?}"),
		}
	}

	fn expect_failed(self, reason: &str) {
		match self {
			AuthResponse::Failed {
				reason: actual,
			} => assert_eq!(actual, reason, "failure reason mismatch"),
			other => panic!("expected failed response with reason '{reason}', got {other:?}"),
		}
	}
}
