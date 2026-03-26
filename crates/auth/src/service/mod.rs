// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Authentication service for ReifyDB.
//!
//! Provides a unified authentication API used by all transports (HTTP, WebSocket,
//! gRPC) and embedded mode. Supports pluggable authentication methods including
//! single-step (password, token) and multi-step challenge-response flows.

mod authenticate;
mod solana;
mod token;

use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};

use reifydb_catalog::{catalog::Catalog, create_token};
use reifydb_core::interface::catalog::token::TokenDef;
use reifydb_runtime::context::{clock::Clock, rng::Rng as SystemRng};
use reifydb_transaction::transaction::{admin::AdminTransaction, query::QueryTransaction};
use reifydb_type::{
	error::Error,
	value::{datetime::DateTime, identity::IdentityId},
};

use crate::{challenge::ChallengeStore, registry::AuthenticationRegistry};

/// Trait abstracting the engine operations needed by the authentication service.
///
/// This allows the auth crate to remain independent of the engine crate while
/// still being able to create transactions and access the catalog.
///
/// All transactions are created with system identity — authentication operates
/// at a privileged level.
pub trait AuthEngine: Send + Sync {
	fn begin_admin(&self) -> Result<AdminTransaction, Error>;
	fn begin_query(&self) -> Result<QueryTransaction, Error>;
	fn catalog(&self) -> Catalog;
}

/// Response from an authentication attempt.
#[derive(Debug, Clone)]
pub enum AuthResponse {
	/// Authentication succeeded. Contains the session token and identity.
	Authenticated {
		identity: IdentityId,
		token: String,
	},
	/// The provider requires a challenge-response round-trip.
	Challenge {
		challenge_id: String,
		payload: HashMap<String, String>,
	},
	/// Authentication failed (wrong credentials, unknown identity, etc.).
	Failed {
		reason: String,
	},
}

/// Configuration for the authentication service.
#[derive(Debug, Clone)]
pub struct AuthServiceConfig {
	/// Default session token TTL. `None` means tokens don't expire.
	pub session_ttl: Option<Duration>,
	/// TTL for pending challenges (default: 60 seconds).
	pub challenge_ttl: Duration,
}

impl Default for AuthServiceConfig {
	fn default() -> Self {
		Self {
			session_ttl: Some(Duration::from_secs(24 * 60 * 60)), // 24 hours
			challenge_ttl: Duration::from_secs(60),
		}
	}
}

impl AuthServiceConfig {
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
}

pub struct Inner {
	pub(crate) engine: Arc<dyn AuthEngine>,
	pub(crate) auth_registry: Arc<AuthenticationRegistry>,
	pub(crate) challenges: ChallengeStore,
	pub(crate) rng: SystemRng,
	pub(crate) clock: Clock,
	pub(crate) session_ttl: Option<Duration>,
}

/// Shared authentication service.
///
/// Coordinates between the identity catalog, authentication providers, and
/// token/challenge stores. All transports and embedded mode call through
/// this single service.
///
/// Cheap to clone — uses `Arc` internally.
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

	/// Get the current time as a DateTime.
	pub(super) fn now(&self) -> DateTime {
		DateTime::from_timestamp_nanos(self.clock.now_nanos())
	}

	/// Compute the expiration DateTime from the configured session TTL.
	pub(super) fn expires_at(&self) -> Option<DateTime> {
		self.session_ttl.map(|ttl| {
			let nanos = self.clock.now_nanos() + ttl.as_nanos();
			DateTime::from_timestamp_nanos(nanos)
		})
	}

	/// Persist a token to the database.
	pub(super) fn persist_token(&self, token: &str, identity: IdentityId) -> Result<TokenDef, Error> {
		let mut admin = self.engine.begin_admin()?;

		let def = create_token(&mut admin, token, identity, self.expires_at(), self.now())?;

		admin.commit()?;
		Ok(def)
	}
}

/// Generate a session token (64 hex characters) using the provided RNG.
pub(super) fn generate_session_token(rng: &SystemRng) -> String {
	let bytes = rng.bytes_32();
	bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
