// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::operator::expectation::Expectation;

pub trait Model<R> {
	type Expectation: Expectation;

	fn admit(&mut self, row: &R) -> bool;

	fn retract(&mut self, row: &R);

	fn update(&mut self, pre: &R, post: &R) {
		self.retract(pre);
		self.admit(post);
	}

	fn advance_ledger(&mut self, at_ms: u64);

	fn live(&self) -> Self::Expectation;

	fn all(&self) -> Self::Expectation;

	fn after_drain(&self) -> Self::Expectation;

	fn step_complete(&mut self) {}
}
