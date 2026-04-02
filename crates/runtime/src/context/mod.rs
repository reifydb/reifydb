// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Runtime context that bundles clock and RNG for the execution engine.

pub mod clock;
pub mod rng;

use clock::{Clock, MockClock};
use rng::Rng;

/// A container for runtime services (clock, RNG) threaded through the execution engine.
#[derive(Clone, Default)]
pub struct RuntimeContext {
	pub clock: Clock,
	pub rng: Rng,
}

impl RuntimeContext {
	/// Create a runtime context with the given clock and RNG.
	pub fn new(clock: Clock, rng: Rng) -> Self {
		Self {
			clock,
			rng,
		}
	}

	/// Create a runtime context with the given clock and OS RNG.
	pub fn with_clock(clock: Clock) -> Self {
		Self {
			clock,
			rng: Rng::default(),
		}
	}

	/// Create a runtime context with a mock clock and seeded RNG (for testing).
	pub fn testing(initial_millis: u64, seed: u64) -> Self {
		Self {
			clock: Clock::Mock(MockClock::from_millis(initial_millis)),
			rng: Rng::seeded(seed),
		}
	}
}
