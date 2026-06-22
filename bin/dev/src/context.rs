// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::context::clock::Clock;

pub struct Context {
	pub clock: Clock,
}

impl Context {
	pub fn new() -> Self {
		Self {
			clock: Clock::Real,
		}
	}
}

impl Default for Context {
	fn default() -> Self {
		Self::new()
	}
}
