// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct KeyedInvertibleAccumulator<K: Ord, A> {
	subs: BTreeMap<K, A>,
}

impl<K: Ord, A> Default for KeyedInvertibleAccumulator<K, A> {
	fn default() -> Self {
		Self {
			subs: BTreeMap::new(),
		}
	}
}

impl<K: Ord, A> KeyedInvertibleAccumulator<K, A> {
	pub fn entries(&self) -> &BTreeMap<K, A> {
		&self.subs
	}
}

impl<K, A> WindowAccumulator for KeyedInvertibleAccumulator<K, A>
where
	K: Ord + Clone + Debug,
	A: WindowAccumulator,
	KeyedInvertibleAccumulator<K, A>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = (K, A::Contribution);
	type Output = BTreeMap<K, A::Output>;

	fn add(&mut self, contribution: &(K, A::Contribution)) {
		self.subs.entry(contribution.0.clone()).or_default().add(&contribution.1);
	}

	fn remove(&mut self, contribution: &(K, A::Contribution)) {
		if let Some(sub) = self.subs.get_mut(&contribution.0) {
			sub.remove(&contribution.1);
			if sub.is_empty() {
				self.subs.remove(&contribution.0);
			}
		}
	}

	fn finalize(&self) -> Option<BTreeMap<K, A::Output>> {
		if self.subs.is_empty() {
			return None;
		}
		let out: BTreeMap<K, A::Output> =
			self.subs.iter().filter_map(|(k, s)| s.finalize().map(|v| (k.clone(), v))).collect();
		(!out.is_empty()).then_some(out)
	}

	fn is_empty(&self) -> bool {
		self.subs.is_empty()
	}
}

impl<K, A> MergeAccumulator for KeyedInvertibleAccumulator<K, A>
where
	K: Ord + Clone + Debug,
	A: MergeAccumulator,
	KeyedInvertibleAccumulator<K, A>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		for (key, sub) in &other.subs {
			self.subs.entry(key.clone()).or_default().merge(sub);
		}
	}
}

impl<K: Ord + HeapSize, A: HeapSize> HeapSize for KeyedInvertibleAccumulator<K, A> {
	fn heap_size(&self) -> usize {
		self.subs.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::state::{OperatorState, decode};

	use super::*;
	use crate::window::accumulator::{invertible::moments::Moments, testkit::assert_add_remove_is_inverse};

	#[test]
	fn keyed_invertible_routes_per_key_and_drops_empty_keys() {
		let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		assert!(accumulator.is_empty());
		assert_eq!(accumulator.finalize(), None);

		accumulator.add(&(1, 10.0));
		accumulator.add(&(1, 20.0));
		accumulator.add(&(2, 5.0));
		let out = accumulator.finalize().expect("non-empty");
		assert_eq!(out.len(), 2);
		assert_eq!(out.get(&1).map(|m| m.sum()), Some(30.0));
		assert_eq!(out.get(&2).map(|m| m.sum()), Some(5.0));

		accumulator.remove(&(2, 5.0));
		let out = accumulator.finalize().expect("non-empty");
		assert_eq!(out.len(), 1, "key 2 drained to empty and was dropped");
		assert!(out.get(&2).is_none());
	}

	#[test]
	fn keyed_invertible_add_remove_is_inverse() {
		assert_add_remove_is_inverse::<KeyedInvertibleAccumulator<u64, Moments>>(
			&[(1u64, 10.0f64), (2, 20.0), (1, 30.0)],
			(3u64, 7.0f64),
		);
	}

	#[test]
	fn keyed_invertible_remove_of_an_unknown_key_is_a_silent_no_op() {
		// Routing swallows what the leaf accumulator would have caught, so a mis-keyed retraction leaves no
		// trace.
		let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		accumulator.add(&(1, 10.0));

		accumulator.remove(&(2, 10.0));

		assert_eq!(accumulator.entries().len(), 1, "the unknown key must never materialise");
		let out = accumulator.finalize().expect("non-empty");
		assert_eq!(out.get(&1).map(|m| m.sum()), Some(10.0), "the live key must be untouched");
	}

	#[test]
	fn keyed_merge_combines_overlapping_keys_and_keeps_disjoint_ones() {
		// A rolling top-k window folds pane accumulators, so a trader seen in two panes must be summed.
		let mut older: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		older.add(&(1, 10.0));
		older.add(&(2, 5.0));
		let mut newer: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		newer.add(&(2, 7.0));
		newer.add(&(3, 1.0));
		let mut whole: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		for c in [(1, 10.0), (2, 5.0), (2, 7.0), (3, 1.0)] {
			whole.add(&c);
		}

		older.merge(&newer);

		assert_eq!(older, whole);
		let out = older.finalize().expect("non-empty");
		assert_eq!(out.get(&1).map(|m| m.sum()), Some(10.0), "a key only in self must survive");
		assert_eq!(out.get(&2).map(|m| m.sum()), Some(12.0), "a key in both must be summed");
		assert_eq!(out.get(&3).map(|m| m.sum()), Some(1.0), "a key only in other must be adopted");
		assert_eq!(newer.entries().len(), 2, "the merged-in accumulator must be left untouched");
	}

	#[test]
	fn keyed_merge_with_an_empty_side_is_the_identity() {
		let mut filled: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		filled.add(&(1, 10.0));
		let before = filled.clone();

		filled.merge(&KeyedInvertibleAccumulator::default());
		assert_eq!(filled, before);

		let mut empty: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		empty.merge(&before);
		assert_eq!(empty, before);
	}

	#[test]
	fn keyed_merge_then_remove_drops_a_key_that_drains() {
		// Eviction retracts contributions from the folded window; a key merged in must drain and vanish.
		let mut folded: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		folded.add(&(1, 10.0));
		let mut pane: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		pane.add(&(2, 4.0));
		folded.merge(&pane);

		folded.remove(&(2, 4.0));

		assert_eq!(folded.entries().len(), 1, "key 2 drained to empty and was dropped");
		assert!(folded.entries().get(&2).is_none());
	}

	#[test]
	fn keyed_invertible_roundtrip() {
		let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		accumulator.add(&(1, 10.0));
		accumulator.add(&(2, 20.0));
		let bytes = accumulator.encode_state().expect("encode");
		let restored: KeyedInvertibleAccumulator<u64, Moments> = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
