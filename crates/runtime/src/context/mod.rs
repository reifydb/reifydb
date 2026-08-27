// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
	pub fn new(clock: Clock, rng: Rng, version_epoch: VersionEpoch) -> Self {
		Self {
			clock,
			rng,
			version_epoch,
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
