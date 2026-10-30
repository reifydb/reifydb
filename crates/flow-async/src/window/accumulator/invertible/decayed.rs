// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::LN_2;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::{
	reifydb_assertions,
	value::{datetime::DateTime, duration::Duration},
};

#[operator_state]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct DecayedSum {
	halflife: Duration,
	reference: Option<DateTime>,
	sum: f64,
	count: u64,
}

impl DecayedSum {
	pub fn new(halflife: Duration) -> Self {
		Self {
			halflife,
			reference: None,
			sum: 0.0,
			count: 0,
		}
	}

	fn weight(&self, from: DateTime, to: DateTime) -> f64 {
		let halflife = self.halflife.as_nanos().expect("halflife in nanoseconds") as f64;
		let age = (to.to_nanos() - from.to_nanos()) as f64;
		(-age * LN_2 / halflife).exp()
	}

	pub fn push(&mut self, at: DateTime, value: f64) {
		match self.reference {
			Some(reference) if at < reference => self.sum += value * self.weight(at, reference),
			Some(reference) => {
				self.sum = self.sum * self.weight(reference, at) + value;
				self.reference = Some(at);
			}
			None => {
				self.sum = value;
				self.reference = Some(at);
			}
		}
		self.count += 1;
	}

	pub fn merge(&mut self, newer: &DecayedSum) {
		reifydb_assertions! {
			assert!(
				self.halflife == newer.halflife,
				"DecayedSum::merge across halflives {:?} and {:?}",
				self.halflife,
				newer.halflife
			);
		}
		let Some(theirs) = newer.reference else {
			return;
		};
		let Some(ours) = self.reference else {
			*self = *newer;
			return;
		};
		let reference = ours.max(theirs);
		self.sum = self.sum * self.weight(ours, reference) + newer.sum * self.weight(theirs, reference);
		self.reference = Some(reference);
		self.count += newer.count;
	}

	pub fn value_at(&self, at: DateTime) -> f64 {
		match self.reference {
			Some(reference) => self.sum * self.weight(reference, at),
			None => 0.0,
		}
	}

	pub fn count(&self) -> u64 {
		self.count
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}
}

impl HeapSize for DecayedSum {
	fn heap_size(&self) -> usize {
		0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const HALFLIFE: i64 = 300;

	fn at(secs: i64) -> DateTime {
		DateTime::from_epoch_secs(1_700_000_000 + secs).unwrap()
	}

	fn halflife() -> Duration {
		Duration::from_seconds(HALFLIFE).unwrap()
	}

	fn pushed(points: &[(i64, f64)]) -> DecayedSum {
		let mut sum = DecayedSum::new(halflife());
		for &(t, v) in points {
			sum.push(at(t), v);
		}
		sum
	}

	fn naive(points: &[(i64, f64)], now: i64) -> f64 {
		points.iter().map(|&(t, v)| v * (-((now - t) as f64) * LN_2 / HALFLIFE as f64).exp()).sum()
	}

	fn close(a: f64, b: f64) -> bool {
		(a - b).abs() <= 1e-9 * b.abs().max(1.0)
	}

	const POINTS: [(i64, f64); 6] = [(0, 100.0), (60, 50.0), (60, 25.0), (420, 300.0), (900, 0.0), (1500, 80.0)];

	#[test]
	fn one_halflife_halves_a_value() {
		let sum = pushed(&[(0, 100.0)]);
		assert!(close(sum.value_at(at(HALFLIFE)), 50.0));
		assert!(close(sum.value_at(at(0)), 100.0));
	}

	#[test]
	fn pushes_in_any_order_match_the_naive_sum() {
		// The newest push moves the reference; an older push must decay to it, not reset it.
		let mut reversed = POINTS;
		reversed.reverse();
		for points in [POINTS, reversed] {
			let sum = pushed(&points);
			assert_eq!(sum.count(), 6);
			assert!(close(sum.value_at(at(1500)), naive(&points, 1500)));
			assert!(close(sum.value_at(at(2000)), naive(&points, 2000)));
		}
	}

	#[test]
	fn every_split_merges_to_the_naive_sum() {
		for cut in 0..=POINTS.len() {
			let mut older = pushed(&POINTS[..cut]);
			older.merge(&pushed(&POINTS[cut..]));
			assert_eq!(older.count(), POINTS.len() as u64, "cut {cut}");
			assert!(close(older.value_at(at(1500)), naive(&POINTS, 1500)), "cut {cut}");
		}
	}

	#[test]
	fn an_empty_sum_is_zero_and_merges_as_identity() {
		let mut empty = DecayedSum::new(halflife());
		assert!(empty.is_empty());
		assert_eq!(empty.value_at(at(0)), 0.0);
		let one = pushed(&[(10, 7.0)]);
		empty.merge(&one);
		assert_eq!(empty, one);
		let mut kept = one;
		kept.merge(&DecayedSum::new(halflife()));
		assert_eq!(kept, one);
	}
}
