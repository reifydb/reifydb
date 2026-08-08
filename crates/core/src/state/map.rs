// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	ops::{Deref, DerefMut},
};

use reifydb_macro::operator_state;
use rkyv::with::AsVec;

use crate::metrics::heap::HeapSize;

#[operator_state]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedMap<K, V> {
	#[rkyv(with = AsVec)]
	entries: BTreeMap<K, V>,
}

impl<K, V> Default for PersistedMap<K, V> {
	fn default() -> Self {
		Self {
			entries: BTreeMap::new(),
		}
	}
}

impl<K, V> Deref for PersistedMap<K, V> {
	type Target = BTreeMap<K, V>;

	fn deref(&self) -> &Self::Target {
		&self.entries
	}
}

impl<K, V> DerefMut for PersistedMap<K, V> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.entries
	}
}

impl<K, V> From<BTreeMap<K, V>> for PersistedMap<K, V> {
	fn from(entries: BTreeMap<K, V>) -> Self {
		Self {
			entries,
		}
	}
}

impl<K, V> From<PersistedMap<K, V>> for BTreeMap<K, V> {
	fn from(map: PersistedMap<K, V>) -> Self {
		map.entries
	}
}

impl<K: Ord, V> FromIterator<(K, V)> for PersistedMap<K, V> {
	fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
		Self {
			entries: BTreeMap::from_iter(iter),
		}
	}
}

impl<K, V> IntoIterator for PersistedMap<K, V> {
	type Item = (K, V);
	type IntoIter = std::collections::btree_map::IntoIter<K, V>;

	fn into_iter(self) -> Self::IntoIter {
		self.entries.into_iter()
	}
}

impl<'a, K, V> IntoIterator for &'a PersistedMap<K, V> {
	type Item = (&'a K, &'a V);
	type IntoIter = std::collections::btree_map::Iter<'a, K, V>;

	fn into_iter(self) -> Self::IntoIter {
		self.entries.iter()
	}
}

impl<K: HeapSize, V: HeapSize> HeapSize for PersistedMap<K, V> {
	fn heap_size(&self) -> usize {
		self.entries.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::operator::{OperatorState, decode};
	use reifydb_value::value::datetime::DateTime;

	use super::PersistedMap;

	#[test]
	fn round_trips_through_the_archived_form() {
		// The whole point of PersistedMap is that its archived layout is a flat pair
		// list (AsVec) rather than rkyv's ordered map, so a key whose archived form is
		// not Ord can still be stored. This pins that the round trip survives.
		let map: PersistedMap<u64, i64> = [(3u64, 30i64), (1, 10), (2, 20)].into_iter().collect();

		let bytes = map.encode_state(DateTime::EPOCH).expect("encode");
		let restored: PersistedMap<u64, i64> = decode(&bytes).expect("decode");

		assert_eq!(restored, map);
		assert_eq!(
			restored.keys().copied().collect::<Vec<_>>(),
			vec![1, 2, 3],
			"ordering must be restored by the BTreeMap on materialize, not by the archived layout"
		);
	}

	#[test]
	fn deref_exposes_the_underlying_map_without_copying() {
		let mut map: PersistedMap<u64, i64> = PersistedMap::default();
		map.insert(7, 70);

		assert_eq!(map.get(&7), Some(&70), "DerefMut/Deref must reach BTreeMap's inherent methods");
		assert_eq!(map.len(), 1);
	}

	#[test]
	fn an_empty_map_round_trips() {
		// Empty is the state every group starts in; if it failed to decode, every first
		// write would have to special-case a missing entry.
		let map: PersistedMap<u64, i64> = PersistedMap::default();
		let bytes = map.encode_state(DateTime::EPOCH).expect("encode");
		let restored: PersistedMap<u64, i64> = decode(&bytes).expect("decode");

		assert!(restored.is_empty());
	}
}
