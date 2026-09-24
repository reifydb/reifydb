// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

const FIRST: Duration = Duration::from_milliseconds_const(500);
const CAP: Duration = Duration::from_seconds_const(30);

#[derive(Clone, Debug)]
pub struct Backoff {
	next: Duration,
}

impl Default for Backoff {
	fn default() -> Self {
		Self {
			next: FIRST,
		}
	}
}

impl Backoff {
	pub fn new() -> Self {
		Self::default()
	}

	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Duration {
		let current = self.next;
		self.next = current.saturating_mul(2).min(CAP);
		current
	}

	pub fn reset(&mut self) {
		self.next = FIRST;
	}
}
