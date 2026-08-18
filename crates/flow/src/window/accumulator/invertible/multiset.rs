// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multiset<V: Ord> {
	counts: BTreeMap<V, u64>,
	total: u64,
}

impl<V: Ord> Default for Multiset<V> {
	fn default() -> Self {
		Self {
			counts: BTreeMap::new(),
			total: 0,
		}
	}
}

impl<V: Ord + Clone> Multiset<V> {
	pub fn add(&mut self, value: V) {
		*self.counts.entry(value).or_insert(0) += 1;
		self.total += 1;
	}

	pub fn remove(&mut self, value: &V) {
		let Some(count) = self.counts.get_mut(value) else {
			#[cfg(reifydb_assertions)]
			panic!("Multiset::remove of absent value");
			#[cfg(not(reifydb_assertions))]
			return;
		};
		*count -= 1;
		self.total -= 1;
		if *count == 0 {
			self.counts.remove(value);
		}
	}

	pub fn merge(&mut self, other: &Self) {
		for (value, count) in &other.counts {
			*self.counts.entry(value.clone()).or_insert(0) += count;
			self.total += count;
		}
	}

	pub fn unmerge(&mut self, other: &Self) {
		for (value, count) in &other.counts {
			let Some(current) = self.counts.get_mut(value) else {
				#[cfg(reifydb_assertions)]
				panic!("Multiset::unmerge of absent value");
				#[cfg(not(reifydb_assertions))]
				continue;
			};
			let dec = (*count).min(*current);
			*current -= dec;
			self.total -= dec;
			if *current == 0 {
				self.counts.remove(value);
			}
		}
	}

	pub fn min(&self) -> Option<&V> {
		self.counts.keys().next()
	}

	pub fn max(&self) -> Option<&V> {
		self.counts.keys().next_back()
	}

	pub fn distinct(&self) -> usize {
		self.counts.len()
	}

	pub fn total(&self) -> u64 {
		self.total
	}

	pub fn is_empty(&self) -> bool {
		self.total == 0
	}

	pub fn mode(&self) -> Option<&V> {
		self.counts
			.iter()
			.reduce(|best, current| {
				if current.1 > best.1 {
					current
				} else {
					best
				}
			})
			.map(|(v, _)| v)
	}

	pub fn quantile(&self, q: f64) -> Option<&V> {
		if self.total == 0 {
			return None;
		}
		let q = q.clamp(0.0, 1.0);
		let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
		let mut cumulative = 0u64;
		for (value, count) in &self.counts {
			cumulative += count;
			if cumulative >= rank {
				return Some(value);
			}
		}
		None
	}

	pub fn median(&self) -> Option<&V> {
		self.quantile(0.5)
	}
}

impl<V: Ord + HeapSize> HeapSize for Multiset<V> {
	fn heap_size(&self) -> usize {
		self.counts.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::pod::state::{OperatorState, decode};

	use super::*;
	use crate::window::accumulator::{
		WindowAccumulator,
		invertible::ordf64::OrdF64,
		testkit::{assert_add_remove_is_inverse, assert_order_independent, of64},
	};

	#[operator_state]
	#[derive(Clone, Debug, Default)]
	struct MinAccumulator {
		values: Multiset<OrdF64>,
	}

	impl HeapSize for MinAccumulator {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for MinAccumulator {
		type Contribution = OrdF64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &OrdF64) {
			self.values.add(*contribution);
		}

		fn remove(&mut self, contribution: &OrdF64) {
			self.values.remove(contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			self.values.min().copied()
		}

		fn is_empty(&self) -> bool {
			self.values.is_empty()
		}
	}

	#[test]
	fn min_add_remove_is_inverse_even_when_probe_is_new_minimum() {
		assert_add_remove_is_inverse::<MinAccumulator>(&[of64(5.0), of64(8.0), of64(6.0)], of64(1.0));
	}

	#[test]
	fn min_add_remove_is_inverse_for_duplicate_value() {
		assert_add_remove_is_inverse::<MinAccumulator>(&[of64(5.0), of64(5.0), of64(8.0)], of64(5.0));
	}

	#[test]
	fn min_is_order_independent() {
		assert_order_independent::<MinAccumulator>(&[of64(3.0), of64(1.0), of64(4.0), of64(1.0), of64(5.0)]);
	}

	#[test]
	fn multiset_min_max_distinct_total() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [5u64, 1, 5, 9, 1] {
			ms.add(v);
		}
		assert_eq!(ms.min(), Some(&1));
		assert_eq!(ms.max(), Some(&9));
		assert_eq!(ms.distinct(), 3);
		assert_eq!(ms.total(), 5);

		ms.remove(&1);
		assert_eq!(ms.min(), Some(&1), "one occurrence of 1 remains");
		assert_eq!(ms.distinct(), 3);
		ms.remove(&1);
		assert_eq!(ms.min(), Some(&5), "last occurrence of 1 removed, min rises");
		assert_eq!(ms.distinct(), 2);
	}

	#[test]
	fn multiset_quantile_and_median_nearest_rank() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [1u64, 2, 3, 4, 5] {
			ms.add(v);
		}
		assert_eq!(ms.quantile(0.0), Some(&1));
		assert_eq!(ms.median(), Some(&3));
		assert_eq!(ms.quantile(1.0), Some(&5));
		assert_eq!(ms.quantile(0.5), Some(&3));
	}

	#[test]
	fn multiset_mode_breaks_ties_to_smallest_value() {
		let mut ms: Multiset<u64> = Multiset::default();
		for v in [7u64, 7, 3, 3, 9] {
			ms.add(v);
		}
		assert_eq!(ms.mode(), Some(&3), "3 and 7 tie at count 2; smallest wins deterministically");
	}

	#[test]
	fn multiset_merge_then_unmerge_restores_the_original() {
		// merge/unmerge is the branch-combine path; any drift between counts and total corrupts every quantile
		// after it.
		let mut base: Multiset<u64> = Multiset::default();
		for v in [1u64, 2, 2, 5] {
			base.add(v);
		}
		let before = base.clone();
		let mut other: Multiset<u64> = Multiset::default();
		for v in [2u64, 7] {
			other.add(v);
		}

		base.merge(&other);
		assert_eq!(base.total(), 6);
		assert_eq!(base.max(), Some(&7));

		base.unmerge(&other);
		assert_eq!(base, before, "unmerge must undo merge exactly");
	}

	#[test]
	fn multiset_unmerge_of_more_than_was_merged_clamps_without_desyncing_the_total() {
		// A count that went negative would wrap, so unmerge clamps; total must still match the counts it kept.
		let mut base: Multiset<u64> = Multiset::default();
		base.add(1);
		base.add(1);
		let mut other: Multiset<u64> = Multiset::default();
		for _ in 0..3 {
			other.add(1);
		}

		base.unmerge(&other);
		assert_eq!(base.total(), 0);
		assert_eq!(base.distinct(), 0, "total and counts must never disagree");
		assert!(base.is_empty());
	}

	#[test]
	fn multiset_roundtrip() {
		let mut ms: Multiset<OrdF64> = Multiset::default();
		ms.add(of64(1.0));
		ms.add(of64(1.0));
		ms.add(of64(2.0));
		let bytes = ms.encode_state().expect("encode");
		let restored: Multiset<OrdF64> = decode(&bytes).expect("decode");
		assert_eq!(restored, ms);
		assert_eq!(restored.min(), Some(&of64(1.0)));
		assert_eq!(restored.total(), 3);
	}
}
