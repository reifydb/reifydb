// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{thread, time::Duration};

use reifydb_core::{execution::ExecutionResult, interface::catalog::token::Token};
use reifydb_runtime::context::rng::Rng;
use reifydb_type::{params::Params, value::identity::IdentityId};
use tracing::{debug, instrument, warn};

use crate::engine::StandardEngine;

pub enum Backoff {
	None,

	Fixed(Duration),

	Exponential {
		base: Duration,
		max: Duration,
	},
	ExponentialJitter {
		base: Duration,
		max: Duration,
	},
}

pub struct RetryStrategy {
	pub max_attempts: u32,
	pub backoff: Backoff,
}

impl Default for RetryStrategy {
	fn default() -> Self {
		Self {
			max_attempts: 10,
			backoff: Backoff::ExponentialJitter {
				base: Duration::from_millis(5),
				max: Duration::from_millis(200),
			},
		}
	}
}

impl RetryStrategy {
	pub fn no_retry() -> Self {
		Self {
			max_attempts: 1,
			backoff: Backoff::None,
		}
	}

	pub fn default_conflict_retry() -> Self {
		Self::default()
	}

	pub fn with_fixed_backoff(max_attempts: u32, delay: Duration) -> Self {
		Self {
			max_attempts,
			backoff: Backoff::Fixed(delay),
		}
	}

	pub fn with_exponential_backoff(max_attempts: u32, base: Duration, max: Duration) -> Self {
		Self {
			max_attempts,
			backoff: Backoff::Exponential {
				base,
				max,
			},
		}
	}

	pub fn with_jittered_backoff(max_attempts: u32, base: Duration, max: Duration) -> Self {
		Self {
			max_attempts,
			backoff: Backoff::ExponentialJitter {
				base,
				max,
			},
		}
	}

	pub fn execute<F>(&self, rng: &Rng, rql: &str, mut f: F) -> ExecutionResult
	where
		F: FnMut() -> ExecutionResult,
	{
		let mut last_result = None;
		for attempt in 0..self.max_attempts {
			let result = f();
			match &result.error {
				None => return result,
				Some(err) if err.code == "TXN_001" => {
					last_result = Some(result);
					let is_last_attempt = attempt + 1 >= self.max_attempts;
					if is_last_attempt {
						warn!(
							attempt = attempt + 1,
							max_attempts = self.max_attempts,
							rql = %rql,
							"Transaction conflict retries exhausted"
						);
					} else {
						let delay = compute_backoff(&self.backoff, attempt, rng);
						debug!(
							attempt = attempt + 1,
							max_attempts = self.max_attempts,
							delay_us = delay.as_micros() as u64,
							rql = %rql,
							"Transaction conflict detected, retrying after backoff"
						);
						if !delay.is_zero() {
							thread::sleep(delay);
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

fn compute_backoff(backoff: &Backoff, attempt: u32, rng: &Rng) -> Duration {
	match backoff {
		Backoff::None => Duration::ZERO,
		Backoff::Fixed(d) => *d,
		Backoff::Exponential {
			base,
			max,
		} => exponential_cap(*base, *max, attempt),
		Backoff::ExponentialJitter {
			base,
			max,
		} => {
			let cap = exponential_cap(*base, *max, attempt);
			let cap_nanos = cap.as_nanos().min(u64::MAX as u128) as u64;
			if cap_nanos == 0 {
				return Duration::ZERO;
			}
			let sampled = rng.infra_u64_inclusive(cap_nanos);
			Duration::from_nanos(sampled)
		}
	}
}

fn exponential_cap(base: Duration, max: Duration, attempt: u32) -> Duration {
	let shift = attempt.min(30);
	let multiplier = 1u32 << shift;
	base.saturating_mul(multiplier).min(max)
}

pub struct Session {
	engine: StandardEngine,
	identity: IdentityId,
	authenticated: bool,
	token: Option<String>,
	retry: RetryStrategy,
}

impl Session {
	pub fn from_token(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	pub fn from_token_with_value(engine: StandardEngine, info: &Token) -> Self {
		Self {
			engine,
			identity: info.identity,
			authenticated: true,
			token: Some(info.token.clone()),
			retry: RetryStrategy::default(),
		}
	}

	pub fn trusted(engine: StandardEngine, identity: IdentityId) -> Self {
		Self {
			engine,
			identity,
			authenticated: false,
			token: None,
			retry: RetryStrategy::default(),
		}
	}

	pub fn anonymous(engine: StandardEngine) -> Self {
		Self::trusted(engine, IdentityId::anonymous())
	}

	pub fn with_retry(mut self, strategy: RetryStrategy) -> Self {
		self.retry = strategy;
		self
	}

	#[inline]
	pub fn identity(&self) -> IdentityId {
		self.identity
	}

	#[inline]
	pub fn token(&self) -> Option<&str> {
		self.token.as_deref()
	}

	#[inline]
	pub fn is_authenticated(&self) -> bool {
		self.authenticated
	}

	#[instrument(name = "session::query", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		self.engine.query_as(self.identity, rql, params.into())
	}

	#[instrument(name = "session::command", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry
			.execute(self.engine.rng(), rql, || self.engine.command_as(self.identity, rql, params.clone()))
	}

	#[instrument(name = "session::admin", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin(&self, rql: &str, params: impl Into<Params>) -> ExecutionResult {
		let params = params.into();
		self.retry.execute(self.engine.rng(), rql, || self.engine.admin_as(self.identity, rql, params.clone()))
	}
}

#[cfg(test)]
mod retry_tests {
	use std::{cell::Cell, time::Duration};

	use reifydb_core::{execution::ExecutionResult, metric::ExecutionMetrics};
	use reifydb_runtime::context::rng::Rng;
	use reifydb_type::{
		error::{Diagnostic, Error},
		fragment::Fragment,
	};

	use super::{Backoff, RetryStrategy, compute_backoff, exponential_cap};

	fn ok() -> ExecutionResult {
		ExecutionResult {
			frames: vec![],
			error: None,
			metrics: ExecutionMetrics::default(),
		}
	}

	fn err(code: &str) -> ExecutionResult {
		ExecutionResult {
			frames: vec![],
			error: Some(Error(Box::new(Diagnostic {
				code: code.to_string(),
				rql: None,
				message: format!("{} test", code),
				column: None,
				fragment: Fragment::None,
				label: None,
				help: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			}))),
			metrics: ExecutionMetrics::default(),
		}
	}

	fn no_sleep_strategy(max_attempts: u32) -> RetryStrategy {
		RetryStrategy {
			max_attempts,
			backoff: Backoff::None,
		}
	}

	#[test]
	fn success_first_try_runs_closure_once() {
		let strategy = no_sleep_strategy(5);
		let rng = Rng::default();
		let calls = Cell::new(0u32);
		let result = strategy.execute(&rng, "", || {
			calls.set(calls.get() + 1);
			ok()
		});
		assert!(result.is_ok());
		assert_eq!(calls.get(), 1);
	}

	#[test]
	fn non_conflict_error_is_not_retried() {
		let strategy = no_sleep_strategy(5);
		let rng = Rng::default();
		let calls = Cell::new(0u32);
		let result = strategy.execute(&rng, "", || {
			calls.set(calls.get() + 1);
			err("TXN_002")
		});
		assert!(result.is_err());
		assert_eq!(calls.get(), 1);
	}

	#[test]
	fn conflict_retries_then_succeeds() {
		let strategy = no_sleep_strategy(5);
		let rng = Rng::default();
		let calls = Cell::new(0u32);
		let result = strategy.execute(&rng, "", || {
			let n = calls.get();
			calls.set(n + 1);
			if n < 2 {
				err("TXN_001")
			} else {
				ok()
			}
		});
		assert!(result.is_ok());
		assert_eq!(calls.get(), 3);
	}

	#[test]
	fn conflict_exhausts_attempts_returns_last_error() {
		let strategy = no_sleep_strategy(4);
		let rng = Rng::default();
		let calls = Cell::new(0u32);
		let result = strategy.execute(&rng, "", || {
			calls.set(calls.get() + 1);
			err("TXN_001")
		});
		assert!(result.is_err());
		assert_eq!(result.error.as_ref().unwrap().code, "TXN_001");
		assert_eq!(calls.get(), 4);
	}

	#[test]
	fn jittered_backoff_stays_within_cap() {
		let base = Duration::from_millis(10);
		let max = Duration::from_millis(100);
		let backoff = Backoff::ExponentialJitter {
			base,
			max,
		};
		let rng = Rng::default();
		for attempt in 0..8 {
			let cap = exponential_cap(base, max, attempt);
			for _ in 0..50 {
				let d = compute_backoff(&backoff, attempt, &rng);
				assert!(d <= cap, "attempt {}: {:?} exceeds cap {:?}", attempt, d, cap);
			}
		}
	}

	#[test]
	fn seeded_rng_produces_deterministic_jitter() {
		let base = Duration::from_millis(5);
		let max = Duration::from_millis(200);
		let backoff = Backoff::ExponentialJitter {
			base,
			max,
		};
		let sample = |seed: u64| -> Vec<Duration> {
			let rng = Rng::seeded(seed);
			(0..8).map(|attempt| compute_backoff(&backoff, attempt, &rng)).collect()
		};
		assert_eq!(sample(42), sample(42));
		assert_ne!(sample(42), sample(43));
	}

	#[test]
	fn seeded_rng_produces_exact_pinned_jitter_values() {
		let base = Duration::from_millis(5);
		let max = Duration::from_millis(200);
		let backoff = Backoff::ExponentialJitter {
			base,
			max,
		};
		let nanos = |seed: u64| -> Vec<u64> {
			let rng = Rng::seeded(seed);
			(0..8).map(|attempt| compute_backoff(&backoff, attempt, &rng).as_nanos() as u64).collect()
		};

		let expected_42: Vec<u64> = vec![
			3_848_394,
			113_809,
			2_934_288,
			23_292_485,
			77_680_508,
			31_066_617,
			36_519_179,
			190_866_841,
		];
		let expected_43: Vec<u64> = vec![
			3_974_671, 4_842_103, 12_057_439, 29_830_325, 72_334_216, 22_229_100, 36_417_439, 81_417_246,
		];

		assert_eq!(nanos(42), expected_42);
		assert_eq!(nanos(43), expected_43);

		assert_eq!(nanos(42), expected_42);
		assert_eq!(nanos(43), expected_43);
	}

	#[test]
	fn exponential_cap_saturates_at_max() {
		let base = Duration::from_millis(5);
		let max = Duration::from_millis(200);
		assert_eq!(exponential_cap(base, max, 0), Duration::from_millis(5));
		assert_eq!(exponential_cap(base, max, 1), Duration::from_millis(10));
		assert_eq!(exponential_cap(base, max, 5), Duration::from_millis(160));
		assert_eq!(exponential_cap(base, max, 6), max);
		assert_eq!(exponential_cap(base, max, 100), max);
	}

	#[test]
	fn default_uses_jittered_backoff() {
		let s = RetryStrategy::default();
		assert_eq!(s.max_attempts, 10);
		match s.backoff {
			Backoff::ExponentialJitter {
				base,
				max,
			} => {
				assert_eq!(base, Duration::from_millis(5));
				assert_eq!(max, Duration::from_millis(200));
			}
			_ => panic!("expected ExponentialJitter default"),
		}
	}
}
