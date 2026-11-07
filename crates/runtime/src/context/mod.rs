// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod clock;
pub mod rng;
pub mod spawner;
pub mod transaction;

use clock::{Clock, MockClock};
use rng::Rng;
use spawner::Spawner;

use crate::version_epoch::VersionEpoch;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
	pub clock: Clock,
	pub rng: Rng,
	pub version_epoch: VersionEpoch,
	pub spawner: Spawner,
}

impl RuntimeContext {
	pub fn builder(spawner: Spawner) -> RuntimeContextBuilder {
		RuntimeContextBuilder {
			clock: Clock::Real,
			rng: Rng::default(),
			version_epoch: VersionEpoch::new(),
			spawner,
		}
	}

	pub fn testing(initial_millis: u64, seed: u64) -> Self {
		Self {
			clock: Clock::Mock(MockClock::from_millis(initial_millis)),
			rng: Rng::seeded(seed),
			version_epoch: VersionEpoch::new(),
			spawner: Spawner::inline(),
		}
	}
}

#[derive(Debug)]
pub struct RuntimeContextBuilder {
	clock: Clock,
	rng: Rng,
	version_epoch: VersionEpoch,
	spawner: Spawner,
}

impl RuntimeContextBuilder {
	pub fn clock(mut self, clock: Clock) -> Self {
		self.clock = clock;
		self
	}

	pub fn rng(mut self, rng: Rng) -> Self {
		self.rng = rng;
		self
	}

	pub fn version_epoch(mut self, version_epoch: VersionEpoch) -> Self {
		self.version_epoch = version_epoch;
		self
	}

	pub fn build(self) -> RuntimeContext {
		RuntimeContext {
			clock: self.clock,
			rng: self.rng,
			version_epoch: self.version_epoch,
			spawner: self.spawner,
		}
	}
}
