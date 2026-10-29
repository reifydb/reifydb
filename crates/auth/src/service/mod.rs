// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod authenticate;
mod github;
mod guest;
mod promote;
mod solana;
mod token;

use std::{collections::HashMap, ops::Deref, sync::Arc};

use reifydb_catalog::{catalog::Catalog, create_session_token, create_token};
use reifydb_core::{interface::catalog::token::Token, retry::RetryStrategy};
use reifydb_runtime::context::{clock::Clock, rng::Rng as SystemRng};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction};
use reifydb_value::{
	error::Error,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::instrument;

use crate::{
	challenge::ChallengeStore,
	github::{GithubApi, GithubConfig, default_api},
	registry::AuthenticationRegistry,
};

pub trait AuthEngine: Send + Sync {
	fn begin_admin(&self) -> Result<AdminTransaction, Error>;
	fn begin_query(&self) -> Result<QueryTransaction, Error>;
	fn catalog(&self) -> Catalog;
}

#[derive(Debug, Clone)]
pub enum AuthResponse {
	Authenticated {
		identity: IdentityId,
		token: String,
	},

	Challenge {
		challenge_id: String,
		payload: HashMap<String, String>,
	},

	Failed {
		reason: String,
	},

	MethodNotEnabled {
		method: String,
	},
}

pub struct AuthConfigurator {
	challenge_ttl: Duration,
	github_client_secret: Option<String>,
}

impl Default for AuthConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl AuthConfigurator {
	pub fn new() -> Self {
		Self {
			challenge_ttl: Duration::from_seconds(60).unwrap(),
			github_client_secret: None,
		}
	}

	pub fn challenge_ttl(mut self, ttl: Duration) -> Self {
		self.challenge_ttl = ttl;
		self
	}

	pub fn github_client_secret(mut self, secret: impl Into<String>) -> Self {
		self.github_client_secret = Some(secret.into());
		self
	}

	pub fn configure(self) -> AuthServiceConfig {
		AuthServiceConfig {
			challenge_ttl: self.challenge_ttl,
			github_client_secret: self.github_client_secret,
		}
	}
}

#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
	pub challenge_ttl: Duration,

	pub github_client_secret: Option<String>,
}

impl Default for AuthServiceConfig {
	fn default() -> Self {
		AuthConfigurator::new().configure()
	}
}

pub struct Inner {
	pub(crate) engine: Arc<dyn AuthEngine>,
	pub(crate) auth_registry: Arc<AuthenticationRegistry>,
	pub(crate) challenges: ChallengeStore,
	pub(crate) rng: SystemRng,
	pub(crate) clock: Clock,
	pub(crate) github_api: Arc<dyn GithubApi>,
}

pub(crate) struct GithubAuth {
	pub(crate) config: GithubConfig,
	pub(crate) api: Arc<dyn GithubApi>,
}

#[derive(Clone)]
pub struct AuthService(Arc<Inner>);

impl Deref for AuthService {
	type Target = Inner;
	fn deref(&self) -> &Inner {
		&self.0
	}
}

impl AuthService {
	pub fn new(
		engine: Arc<dyn AuthEngine>,
		auth_registry: Arc<AuthenticationRegistry>,
		rng: SystemRng,
		clock: Clock,
		config: AuthServiceConfig,
	) -> Self {
		Self::with_github_api(engine, auth_registry, rng, clock, config, default_api())
	}

	pub fn with_github_api(
		engine: Arc<dyn AuthEngine>,
		auth_registry: Arc<AuthenticationRegistry>,
		rng: SystemRng,
		clock: Clock,
		config: AuthServiceConfig,
		api: Arc<dyn GithubApi>,
	) -> Self {
		Self(Arc::new(Inner {
			engine,
			auth_registry,
			challenges: ChallengeStore::new(config.challenge_ttl),
			rng,
			clock,
			github_api: api,
		}))
	}

	pub fn auth_registry(&self) -> &Arc<AuthenticationRegistry> {
		&self.auth_registry
	}

	pub(super) fn now(&self) -> Result<DateTime, Error> {
		Ok(self.clock.now())
	}

	pub(super) fn expires_at(&self, ttl: Option<Duration>) -> Result<Option<DateTime>, Error> {
		match ttl {
			Some(ttl) => {
				let ttl_nanos = ttl.as_nanos()?;
				let nanos = self.clock.now().to_nanos().saturating_add(ttl_nanos);
				Ok(Some(DateTime::from_nanos(nanos)))
			}
			None => Ok(None),
		}
	}

	pub(super) fn persist_token(
		&self,
		token: &str,
		identity: IdentityId,
		ttl: Option<Duration>,
	) -> Result<Token, Error> {
		RetryStrategy::default().retry(&self.rng, "auth::persist_token", || {
			let mut admin = self.engine.begin_admin()?;

			let def =
				create_session_token(&mut admin, token, identity, self.expires_at(ttl)?, self.now()?)?;

			admin.commit()?;
			Ok(def)
		})
	}

	pub fn create_token(
		&self,
		token: &str,
		identity: IdentityId,
		expires_at: Option<DateTime>,
	) -> Result<Token, Error> {
		RetryStrategy::default().retry(&self.rng, "auth::create_token", || {
			let mut admin = self.engine.begin_admin()?;
			let def = create_token(&mut admin, token, identity, expires_at, self.now()?)?;
			admin.commit()?;
			Ok(def)
		})
	}

	#[instrument(name = "auth::create_session", level = "debug", skip(self))]
	pub fn create_session(&self, identity: IdentityId, ttl: Option<Duration>) -> Result<Token, Error> {
		self.persist_token(&generate_session_token(&self.rng), identity, ttl)
	}

	pub(super) fn set_lookup_attribute(
		&self,
		admin: &mut AdminTransaction,
		identity: IdentityId,
		name: &str,
		value: &str,
	) -> Result<(), Error> {
		let catalog = self.engine.catalog();
		let attribute =
			match catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut *admin), name)? {
				Some(attribute) => attribute,
				None => catalog.create_identity_attribute(admin, name, ValueType::Utf8)?,
			};
		catalog.set_identity_attribute_value(admin, identity, &attribute, Value::Utf8(value.to_string()))?;
		Ok(())
	}
}

pub(super) fn generate_session_token(rng: &SystemRng) -> String {
	let bytes = rng.infra_bytes_32();
	bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
