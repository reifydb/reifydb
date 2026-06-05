// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

//! Sources of non-determinism the workspace consumes: the wall clock and the random number generator. Both have
//! mockable variants so a deterministic-simulation run replaces them with seeded equivalents and reproduces the
//! same trace bit-for-bit. Anything in the workspace that needs the time of day or a random value reaches for
//! these handles instead of pulling from `std`.

pub mod clock;
pub mod rng;

use clock::{Clock, MockClock};
use rng::Rng;

use crate::version_epoch::VersionEpoch;

#[derive(Clone)]
pub struct RuntimeContext {
	pub clock: Clock,
	pub rng: Rng,
	pub version_epoch: VersionEpoch,
}

impl RuntimeContext {
	pub fn new(clock: Clock, rng: Rng) -> Self {
		Self {
			clock,
			rng,
			version_epoch: VersionEpoch::new(),
		}
	}

	pub fn with_clock(clock: Clock) -> Self {
		Self {
			clock,
			rng: Rng::default(),
			version_epoch: VersionEpoch::new(),
		}
	}

	pub fn testing(initial_millis: u64, seed: u64) -> Self {
		Self {
			clock: Clock::Mock(MockClock::from_millis(initial_millis)),
			rng: Rng::seeded(seed),
			version_epoch: VersionEpoch::new(),
		}
	}
}
