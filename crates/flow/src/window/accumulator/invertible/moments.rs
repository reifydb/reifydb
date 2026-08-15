// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::reifydb_assertions;

use crate::window::accumulator::WindowAccumulator;

#[operator_state]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Moments {
	n: u64,
	sum: f64,
	sum_sq: f64,
}

impl WindowAccumulator for Moments {
	type Contribution = f64;
	type Output = Moments;

	fn add(&mut self, contribution: &f64) {
		Moments::add(self, *contribution);
	}

	fn remove(&mut self, contribution: &f64) {
		Moments::remove(self, *contribution);
	}

	fn finalize(&self) -> Option<Moments> {
		(self.n > 0).then_some(*self)
	}

	fn is_empty(&self) -> bool {
		self.n == 0
	}
}

impl Moments {
	#[inline]
	pub fn add(&mut self, x: f64) {
		self.n += 1;
		self.sum += x;
		self.sum_sq += x * x;
	}

	#[inline]
	pub fn remove(&mut self, x: f64) {
		reifydb_assertions! {
			assert!(self.n > 0, "Moments::remove on empty accumulator");
		}
		self.n -= 1;
		if self.n == 0 {
			self.sum = 0.0;
			self.sum_sq = 0.0;
			return;
		}
		self.sum -= x;
		self.sum_sq -= x * x;
	}

	#[inline]
	pub fn count(&self) -> u64 {
		self.n
	}

	#[inline]
	pub fn sum(&self) -> f64 {
		self.sum
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.n == 0
	}

	pub fn mean(&self) -> Option<f64> {
		(self.n > 0).then(|| self.sum / self.n as f64)
	}

	pub fn variance_pop(&self) -> Option<f64> {
		(self.n > 0).then(|| {
			let mean = self.sum / self.n as f64;
			(self.sum_sq / self.n as f64 - mean * mean).max(0.0)
		})
	}

	pub fn stddev_pop(&self) -> Option<f64> {
		self.variance_pop().map(f64::sqrt)
	}
}

impl HeapSize for Moments {
	fn heap_size(&self) -> usize {
		0
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::{OperatorState, decode};
	use reifydb_macro::operator_state;
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::window::accumulator::testkit::{assert_add_remove_is_inverse, assert_order_independent};

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	struct SumAccumulator {
		moments: Moments,
	}

	impl HeapSize for SumAccumulator {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for SumAccumulator {
		type Contribution = f64;
		type Output = f64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<f64> {
			(!self.moments.is_empty()).then(|| self.moments.sum())
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[test]
	fn sum_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<SumAccumulator>(&[1.0, 2.0, 3.0], 7.0);
	}

	#[test]
	fn sum_is_order_independent() {
		assert_order_independent::<SumAccumulator>(&[1.0, 2.0, 4.0, 8.0]);
	}

	#[test]
	fn moments_drains_to_exact_zero() {
		let mut m = Moments::default();
		m.add(0.1);
		m.add(0.2);
		m.remove(0.1);
		m.remove(0.2);
		assert_eq!(m.count(), 0);
		assert_eq!(m.sum(), 0.0, "fully drained accumulator resets sum to exact zero");
		assert!(m.is_empty());
		assert_eq!(m.mean(), None);
		assert_eq!(m.variance_pop(), None);
	}

	#[test]
	fn moments_mean_and_variance() {
		let mut m = Moments::default();
		for x in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
			m.add(x);
		}
		assert_eq!(m.count(), 8);
		assert_eq!(m.mean(), Some(5.0));
		assert_eq!(m.variance_pop(), Some(4.0));
		assert_eq!(m.stddev_pop(), Some(2.0));
	}

	#[test]
	fn moments_variance_is_zero_for_a_lone_survivor_of_large_values() {
		// sum_sq carries the rounding error of every value ever added, and one observation has no spread.
		let mut m = Moments::default();
		m.add(1.0e8);
		m.add(1.0e8 + 1.0);
		m.add(1.0e8 + 2.0);
		m.remove(1.0e8 + 1.0);
		m.remove(1.0e8 + 2.0);

		assert_eq!(m.count(), 1);
		assert_eq!(m.variance_pop(), Some(0.0));
		assert_eq!(m.stddev_pop(), Some(0.0));
	}

	#[test]
	fn moments_roundtrip() {
		let mut m = Moments::default();
		m.add(1.5);
		m.add(2.5);
		let bytes = m.encode_state(DateTime::EPOCH).expect("encode");
		let restored: Moments = decode(&bytes).expect("decode");
		assert_eq!(restored, m);
	}
}
