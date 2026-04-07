// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Unified session type for database access.
//!
//! A `Session` binds an identity to an engine and provides query, command, and
//! admin methods. Sessions are created either from a validated auth token
//! (server path) or directly from an `IdentityId` (embedded/trusted path).

use std::{thread, time::Duration};

use reifydb_core::{execution::ExecutionResult, interface::catalog::token::Token};
use reifydb_type::{params::Params, value::identity::IdentityId};
use tracing::{instrument, warn};

use crate::engine::StandardEngine;

/// Backoff strategy between retry attempts.
pub enum Backoff {
	/// No delay between retries.
	None,
	/// Fixed delay between each retry attempt.
	Fixed(Duration),
	/// Exponential backoff: delay doubles each attempt, capped at `max`.
	Exponential {
		base: Duration,
		max: Duration,
	},
}

/// Controls how many times a write transaction is retried on conflict (`TXN_001`).
pub struct RetryStrategy {
	pub max_attempts: u32,
	pub backoff: Backoff,
}

impl Default for RetryStrategy {
	fn default() -> Self {
		Self {
			max_attempts: 3,
			backoff: Backoff::None,
		}
	}
}

impl RetryStrategy {
	/// No retries — fail immediately on conflict.
	pub fn no_retry() -> Self {
		Self {
			max_attempts: 1,
			backoff: Backoff::None,
		}
	}

	/// Default conflict retry: 3 attempts, no backoff (matches legacy engine behavior).
	pub fn default_conflict_retry() -> Self {
		Self::default()
	}

	/// Fixed delay between retry attempts.
	pub fn with_fixed_backoff(max_attempts: u32, delay: Duration) -> Self {
		Self {
			max_attempts,
			backoff: Backoff::Fixed(delay),
		}
	}

	/// Exponential backoff: delay doubles each attempt, capped at `max`.
	pub fn with_exponential_backoff(max_attempts: u32, base: Duration, max: Duration) -> Self {
		Self {
			max_attempts,
			backoff: Backoff::Exponential {
				base,
				max,
			},
		}
	}

	pub fn execute<F>(&self, _rql: &str, mut f: F) -> ExecutionResult
	where
		F: FnMut() -> ExecutionResult,
	{
		let mut last_result = None;
		for attempt in 0..self.max_attempts {
			let result = f();
			match &result.error {
				None => return result,
				Some(err) if err.code == "TXN_001" => {
					warn!(attempt = attempt + 1, "Transaction conflict detected, retrying");
					last_result = Some(result);
					if attempt + 1 < self.max_attempts {
						match &self.backoff {
							Backoff::None => {}
							Backoff::Fixed(d) => thread::sleep(*d),
							Backoff::Exponential {
								base,
								max,
							} => {
								let delay = (*base) * 2u32.saturating_pow(attempt);
								thread::sleep(delay.min(*max));
							}
						}
					}
				}
				Some(_) => {
					return result;
				}
			}
		}
		last_result.unwrap()
	}
}

/// A unified session binding an identity to a database engine.
pub struct Session {
	engine: StandardEngine,
	identity: IdentityId,
	authenticated: bool,
	token: Option<String>,
	retry: RetryStrategy,
}

impl Session {
	/// Create a session from a validated auth token (server path).
	pub fn from_token(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	/// Create a session from a validated auth token, preserving the token string.
	pub fn from_token_with_value(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: Some(info.token.clone()),
			retry: RetryStrategy::default(),
		}
	}

	/// Create a trusted session (embedded path, no authentication required).
	pub fn trusted(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			authenticated: false,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	/// Create an anonymous session.
	pub fn anonymous(engine: StandardEngine) -> Self {
		Self::trusted(engine, IdentityId::anonymous())
	}

	/// Set the retry strategy for command and admin operations.
	pub fn with_retry(mut self, strategy: RetryStrategy) -> Self {
		self.retry = strategy;
		self
	}

	/// The identity associated with this session.
	#[inline]
	pub fn identity(&self) -> IdentityId {
		self.identity
	}

	/// The auth token, if this session was created from a validated token.
	#[inline]
	pub fn token(&self) -> Option<&str> {
		self.token.as_deref()
	}

	/// Whether this session was created from authenticated credentials.
	#[inline]
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}

	/// Execute a read-only query.
	#[instrument(name = "session::query", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		self.engine.query_as(self.identity, rql, params.into())
	}

	/// Execute a transactional command (DML + Query) with retry on conflict.
	#[instrument(name = "session::command", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry.execute(rql, || self.engine.command_as(self.identity, rql, params.clone()))
	}

	/// Execute an admin (DDL + DML + Query) operation with retry on conflict.
	#[instrument(name = "session::admin", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry.execute(rql, || self.engine.admin_as(self.identity, rql, params.clone()))
	}
}
