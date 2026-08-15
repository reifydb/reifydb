// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, fmt::Debug};

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::WindowAccumulator;

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

impl<K: Ord + HeapSize, A: HeapSize> HeapSize for KeyedInvertibleAccumulator<K, A> {
	fn heap_size(&self) -> usize {
		self.subs.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::{OperatorState, decode};
	use reifydb_value::value::datetime::DateTime;

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
	fn keyed_invertible_roundtrip() {
		let mut accumulator: KeyedInvertibleAccumulator<u64, Moments> = KeyedInvertibleAccumulator::default();
		accumulator.add(&(1, 10.0));
		accumulator.add(&(2, 20.0));
		let bytes = accumulator.encode_state(DateTime::EPOCH).expect("encode");
		let restored: KeyedInvertibleAccumulator<u64, Moments> = decode(&bytes).expect("decode");
		assert_eq!(restored, accumulator);
	}
}
