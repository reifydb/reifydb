// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod clock;
pub mod rng;

use clock::{Clock, MockClock};
use rng::Rng;

#[derive(Clone)]
pub struct RuntimeContext {
	pub clock: Clock,
	pub rng: Rng,
}

impl RuntimeContext {
	pub fn new(clock: Clock, rng: Rng) -> Self {
		Self {
			clock,
			rng,
		}
	}

	pub fn with_clock(clock: Clock) -> Self {
		Self {
			clock,
			rng: Rng::default(),
		}
	}

	pub fn testing(initial_millis: u64, seed: u64) -> Self {
		Self {
			clock: Clock::Mock(MockClock::from_millis(initial_millis)),
			rng: Rng::seeded(seed),
		}
	}
}
