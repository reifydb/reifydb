// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Runtime context that bundles clock and RNG for the execution engine.

pub mod clock;
pub mod rng;

use clock::Clock;
use rng::Rng;

/// A container for runtime services (clock, RNG) threaded through the execution engine.
#[derive(Clone)]
pub struct RuntimeContext {
	pub clock: Clock,
	pub rng: Rng,
}

impl Default for RuntimeContext {
	fn default() -> Self {
		Self {
			clock: Clock::default(),
			rng: Rng::default(),
		}
	}
}

impl RuntimeContext {
	/// Create a runtime context with the given clock and OS RNG.
	pub fn with_clock(clock: Clock) -> Self {
		Self {
			clock,
			rng: Rng::default(),
		}
	}

	/// Create a runtime context with a mock clock and seeded RNG (for testing).
	pub fn testing(initial_millis: u64, seed: u64) -> Self {
		use clock::MockClock;
		Self {
			clock: Clock::Mock(MockClock::from_millis(initial_millis)),
			rng: Rng::seeded(seed),
		}
	}
}
