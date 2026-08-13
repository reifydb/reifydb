// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::WindowAccumulator;

#[operator_state]
#[derive(Clone, Debug, Default)]
pub(crate) struct SumAccumulator {
	pub sum: i64,
	pub count: u64,
}

impl HeapSize for SumAccumulator {
	fn heap_size(&self) -> usize {
		0
	}
}

impl WindowAccumulator for SumAccumulator {
	type Contribution = i64;
	type Output = i64;

	fn add(&mut self, contribution: &i64) {
		self.sum += *contribution;
		self.count += 1;
	}
	fn remove(&mut self, contribution: &i64) {
		self.sum -= *contribution;
		self.count = self.count.saturating_sub(1);
	}
	fn finalize(&self) -> Option<i64> {
		if self.count == 0 {
			None
		} else {
			Some(self.sum)
		}
	}
	fn is_empty(&self) -> bool {
		self.count == 0
	}
	fn merge(&mut self, other: &Self) {
		self.sum += other.sum;
		self.count += other.count;
	}
	fn unmerge(&mut self, other: &Self) {
		self.sum -= other.sum;
		self.count = self.count.saturating_sub(other.count);
	}
}
