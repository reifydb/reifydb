// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Retractions(AtomicU64);

impl Retractions {
	pub fn new() -> Self {
		Self(AtomicU64::new(0))
	}

	pub fn token(&self) -> u64 {
		self.0.load(Ordering::SeqCst)
	}

	pub fn record(&self) {
		self.0.fetch_add(1, Ordering::SeqCst);
	}

	pub fn unchanged(&self, token: u64) -> bool {
		self.token() == token
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_fresh_counter_starts_at_zero() {
		// A non-zero start makes the first fill's token disagree with the counter it was read from, so the
		// first claim of a tier's life is refused for a withdrawal that never happened.
		let retractions = Retractions::new();
		assert_eq!(retractions.token(), 0);
		assert!(retractions.unchanged(0));
	}

	#[test]
	fn default_matches_new() {
		// A derived default that started elsewhere would give two tiers built by different paths different
		// tokens for the same untouched state.
		assert_eq!(Retractions::default().token(), Retractions::new().token());
	}

	#[test]
	fn record_moves_the_token() {
		// A withdrawal that leaves the token still would let a fill already in flight publish a claim over
		// the rows the withdrawal just dropped.
		let retractions = Retractions::new();
		let token = retractions.token();
		retractions.record();
		assert_ne!(retractions.token(), token);
		assert!(!retractions.unchanged(token));
	}

	#[test]
	fn every_record_moves_the_token_again() {
		// Saturating or wrapping back onto an earlier value would make a stale token compare equal, which
		// is the one comparison that must never pass by accident.
		let retractions = Retractions::new();
		let mut seen = vec![retractions.token()];
		for _ in 0..8 {
			retractions.record();
			let token = retractions.token();
			assert!(!seen.contains(&token));
			seen.push(token);
		}
	}

	#[test]
	fn unchanged_holds_across_a_read_that_records_nothing() {
		// Reading the token must not itself count as a withdrawal, or no fill could ever publish.
		let retractions = Retractions::new();
		retractions.record();
		let token = retractions.token();
		assert_eq!(retractions.token(), token);
		assert!(retractions.unchanged(token));
	}
}
