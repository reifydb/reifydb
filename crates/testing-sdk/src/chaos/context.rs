// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter};

use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_value::value::{Value, datetime::DateTime};

#[derive(Clone)]
pub struct ChaosContext {
	pub seed: u64,
	pub clock: Clock,
	pub drain_at_ms: u64,
	pub dictionaries: Vec<(Dictionary, Vec<Value>)>,
}

impl ChaosContext {
	pub fn new(seed: u64) -> Self {
		Self {
			seed,
			clock: Clock::Mock(MockClock::new(seed & i64::MAX as u64)),
			drain_at_ms: 0,
			dictionaries: Vec::new(),
		}
	}

	pub fn now(&self) -> DateTime {
		self.clock.now()
	}
}

impl Debug for ChaosContext {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ChaosContext")
			.field("seed", &self.seed)
			.field("now", &self.now())
			.field("drain_at_ms", &self.drain_at_ms)
			.finish()
	}
}
