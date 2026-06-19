// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter};

use reifydb_runtime::context::clock::{Clock, MockClock};

pub struct ChaosContext {
	pub seed: u64,
	pub clock: Clock,
}

impl ChaosContext {
	pub fn new(seed: u64) -> Self {
		Self {
			seed,
			clock: Clock::Mock(MockClock::new(seed)),
		}
	}

	pub fn now_nanos(&self) -> u64 {
		self.clock.now_nanos()
	}
}

impl Debug for ChaosContext {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ChaosContext").field("seed", &self.seed).field("now_nanos", &self.now_nanos()).finish()
	}
}
