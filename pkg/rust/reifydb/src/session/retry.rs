// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_type::{error::Error, value::frame::frame::Frame};

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
pub struct RetryPolicy {
	pub max_attempts: u32,
	pub backoff: Backoff,
}

impl Default for RetryPolicy {
	fn default() -> Self {
		Self {
			max_attempts: 3,
			backoff: Backoff::None,
		}
	}
}

impl RetryPolicy {
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

	/// Execute `f` with retry on `TXN_001` conflict errors.
	pub(crate) fn execute<F>(&self, rql: &str, mut f: F) -> Result<Vec<Frame>, Error>
	where
		F: FnMut() -> Result<Vec<Frame>, Error>,
	{
		let mut last_err = None;
		for attempt in 0..self.max_attempts {
			match f() {
				Ok(frames) => return Ok(frames),
				Err(err) if err.code == "TXN_001" => {
					tracing::warn!(
						attempt = attempt + 1,
						"Transaction conflict detected, retrying"
					);
					last_err = Some(err);
					if attempt + 1 < self.max_attempts {
						match &self.backoff {
							Backoff::None => {}
							Backoff::Fixed(d) => std::thread::sleep(*d),
							Backoff::Exponential {
								base,
								max,
							} => {
								let delay = (*base) * 2u32.saturating_pow(attempt);
								std::thread::sleep(delay.min(*max));
							}
						}
					}
				}
				Err(mut err) => {
					err.with_statement(rql.to_string());
					return Err(err);
				}
			}
		}
		let mut err = last_err.unwrap();
		err.with_statement(rql.to_string());
		Err(err)
	}
}
