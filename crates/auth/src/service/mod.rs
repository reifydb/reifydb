// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod authenticate;
mod solana;
mod token;

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use reifydb_catalog::{catalog::Catalog, create_token};
use reifydb_core::interface::catalog::token::Token;
use reifydb_runtime::context::{clock::Clock, rng::Rng as SystemRng};
use reifydb_transaction::transaction::{admin::AdminTransaction, query::QueryTransaction};
use reifydb_type::{
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};

use crate::{challenge::ChallengeStore, registry::AuthenticationRegistry};

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
}

pub struct AuthConfigurator {
	session_ttl: Option<Duration>,
	challenge_ttl: Duration,
}

impl Default for AuthConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl AuthConfigurator {
	pub fn new() -> Self {
		Self {
			session_ttl: Some(Duration::from_secs(24 * 60 * 60)),
			challenge_ttl: Duration::from_secs(60),
		}
	}

	pub fn session_ttl(mut self, ttl: Duration) -> Self {
		self.session_ttl = Some(ttl);
		self
	}

	pub fn no_session_ttl(mut self) -> Self {
		self.session_ttl = None;
		self
	}

	pub fn challenge_ttl(mut self, ttl: Duration) -> Self {
		self.challenge_ttl = ttl;
		self
	}

	pub fn configure(self) -> AuthServiceConfig {
		AuthServiceConfig {
			session_ttl: self.session_ttl,
			challenge_ttl: self.challenge_ttl,
		}
	}
}

#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
	pub session_ttl: Option<Duration>,

	pub challenge_ttl: Duration,
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
	pub(crate) session_ttl: Option<Duration>,
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
		Self(Arc::new(Inner {
			engine,
			auth_registry,
			challenges: ChallengeStore::new(config.challenge_ttl),
			rng,
			clock,
			session_ttl: config.session_ttl,
		}))
	}

	pub(super) fn now(&self) -> Result<DateTime, Error> {
		Ok(DateTime::from_nanos(self.clock.now_nanos()))
	}

	pub(super) fn expires_at(&self) -> Result<Option<DateTime>, Error> {
		match self.session_ttl {
			Some(ttl) => {
				let ttl_nanos = ttl.as_nanos() as u64;
				let nanos = self.clock.now_nanos().saturating_add(ttl_nanos);
				Ok(Some(DateTime::from_nanos(nanos)))
			}
			None => Ok(None),
		}
	}

	pub(super) fn persist_token(&self, token: &str, identity: IdentityId) -> Result<Token, Error> {
		let mut admin = self.engine.begin_admin()?;

		let def = create_token(&mut admin, token, identity, self.expires_at()?, self.now()?)?;

		admin.commit()?;
		Ok(def)
	}

	pub fn create_token(
		&self,
		token: &str,
		identity: IdentityId,
		expires_at: Option<DateTime>,
	) -> Result<Token, Error> {
		let mut admin = self.engine.begin_admin()?;
		let def = create_token(&mut admin, token, identity, expires_at, self.now()?)?;
		admin.commit()?;
		Ok(def)
	}
}

pub(super) fn generate_session_token(rng: &SystemRng) -> String {
	let bytes = rng.infra_bytes_32();
	bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
