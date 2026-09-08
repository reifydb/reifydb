// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, fmt::Debug, hash::Hash, ops::Bound};

use reifydb_codec::key::encoded::EncodedKey;
pub use reifydb_macro::{KeyCodec, KeyLayout};

use crate::metrics::heap::HeapSize;

pub mod direction;
pub mod key;
pub mod layout;
pub mod range;

pub trait Key: Clone + Ord + Hash + Debug + HeapSize + Send + Sync + 'static {}

impl<T> Key for T where T: Clone + Ord + Hash + Debug + HeapSize + Send + Sync + 'static {}

pub trait BoundedKey: Key {
	fn low() -> Self;
}

pub trait DenseKey: Key {
	fn successor(&self) -> Option<Self>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edge<K> {
	Bottom,
	Key(K),
	AfterKey(K),
	Top,
}

impl<K: HeapSize> HeapSize for Edge<K> {
	fn heap_size(&self) -> usize {
		match self {
			Edge::Key(key) | Edge::AfterKey(key) => key.heap_size(),
			Edge::Bottom | Edge::Top => 0,
		}
	}
}

impl<K: DenseKey> Edge<K> {
	pub fn just_past(key: &K) -> Self {
		match key.successor() {
			Some(next) => Edge::Key(next),
			None => Edge::AfterKey(key.clone()),
		}
	}
}

impl<K: Clone> Edge<K> {
	pub fn lower_bound(&self) -> Option<Bound<K>> {
		match self {
			Edge::Bottom => Some(Bound::Unbounded),
			Edge::Key(key) => Some(Bound::Included(key.clone())),
			Edge::AfterKey(key) => Some(Bound::Excluded(key.clone())),
			Edge::Top => None,
		}
	}

	pub fn upper_bound(&self) -> Option<Bound<K>> {
		match self {
			Edge::Bottom => None,
			Edge::Key(key) => Some(Bound::Excluded(key.clone())),
			Edge::AfterKey(key) => Some(Bound::Included(key.clone())),
			Edge::Top => Some(Bound::Unbounded),
		}
	}
}

impl<K: BoundedKey> Edge<K> {
	pub fn anchor(&self) -> Option<K> {
		match self {
			Edge::Bottom => Some(K::low()),
			Edge::Key(key) | Edge::AfterKey(key) => Some(key.clone()),
			Edge::Top => None,
		}
	}

	pub fn lowest(&self) -> Option<K> {
		match self {
			Edge::Bottom => Some(K::low()),
			Edge::Key(key) => Some(key.clone()),
			Edge::AfterKey(_) | Edge::Top => None,
		}
	}
}

impl Edge<OpaqueKey> {
	pub fn of(key: impl AsRef<[u8]>) -> Self {
		Edge::Key(EncodedKey::new(key))
	}
}

impl<K: Ord> Edge<K> {
	pub fn cmp_key(&self, key: &K) -> Ordering {
		match self {
			Edge::Bottom => Ordering::Less,
			Edge::Key(edge) => edge.cmp(key),
			Edge::AfterKey(edge) => match edge.cmp(key) {
				Ordering::Less => Ordering::Less,
				Ordering::Equal | Ordering::Greater => Ordering::Greater,
			},
			Edge::Top => Ordering::Greater,
		}
	}

	pub fn covers(&self, key: &K) -> bool {
		self.cmp_key(key) == Ordering::Greater
	}

	pub fn admits(&self, key: &K) -> bool {
		self.cmp_key(key) != Ordering::Greater
	}

	pub fn min(self, other: Self) -> Self {
		if self <= other {
			self
		} else {
			other
		}
	}
}

impl<K: Ord> PartialOrd for Edge<K> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<K: Ord> Ord for Edge<K> {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Edge::Bottom, Edge::Bottom) => Ordering::Equal,
			(Edge::Bottom, _) => Ordering::Less,
			(_, Edge::Bottom) => Ordering::Greater,
			(Edge::Top, Edge::Top) => Ordering::Equal,
			(Edge::Top, _) => Ordering::Greater,
			(_, Edge::Top) => Ordering::Less,
			(Edge::Key(left), Edge::Key(right)) => left.cmp(right),
			(Edge::AfterKey(left), Edge::AfterKey(right)) => left.cmp(right),
			(Edge::Key(left), Edge::AfterKey(right)) => left.cmp(right).then(Ordering::Less),
			(Edge::AfterKey(left), Edge::Key(right)) => left.cmp(right).then(Ordering::Greater),
		}
	}
}

pub type OpaqueKey = EncodedKey;

impl BoundedKey for () {
	fn low() -> Self {}
}

impl DenseKey for () {
	fn successor(&self) -> Option<Self> {
		None
	}
}

impl BoundedKey for EncodedKey {
	fn low() -> Self {
		EncodedKey::new([])
	}
}

impl DenseKey for EncodedKey {
	fn successor(&self) -> Option<Self> {
		let mut bytes = Vec::with_capacity(self.as_slice().len() + 1);
		bytes.extend_from_slice(self.as_slice());
		bytes.push(0);
		Some(EncodedKey::new(bytes))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;

	use super::{BoundedKey, DenseKey, Edge, OpaqueKey};

	#[test]
	fn unit_key_has_no_successor() {
		// a group only keyspace subtracts its whole key, so the empty key must report the top of its space
		assert_eq!(<() as BoundedKey>::low(), ());
		assert_eq!(<() as DenseKey>::successor(&()), None);
	}

	#[test]
	fn encoded_key_low_is_empty() {
		assert_eq!(<OpaqueKey as BoundedKey>::low().as_slice(), &[] as &[u8]);
	}

	#[test]
	fn encoded_key_successor_appends_a_zero_byte() {
		// store-multi's coverage successor is byte append; a different rule here would resize every
		// interval it has already proven
		let key = EncodedKey::new([0x01, 0x02]);
		assert_eq!(key.successor().unwrap().as_slice(), &[0x01, 0x02, 0x00]);
	}

	#[test]
	fn encoded_key_successor_never_runs_out() {
		// byte strings have no greatest element, so none here would claim coverage that was never proven
		let all_ones = EncodedKey::new([0xff, 0xff, 0xff]);
		assert!(all_ones.successor().is_some());
		assert!(EncodedKey::new([]).successor().is_some());
	}

	#[test]
	fn encoded_key_successor_is_the_immediate_next_key() {
		// nothing may sort between a key and its successor, otherwise an exclusive upper end drops a row
		let key = EncodedKey::new([0x01]);
		let successor = key.successor().unwrap();
		assert!(successor > key);
		assert!(EncodedKey::new([0x01, 0x00, 0x00]) > successor);
		assert!(EncodedKey::new([0x02]) > successor);
	}

	#[test]
	fn exclusive_upper_end_carries_a_key_or_the_top() {
		let end: Edge<OpaqueKey> = Edge::Key(EncodedKey::new([0x01]));
		assert_ne!(end, Edge::Top);
		assert_eq!(end.clone(), end);
	}

	#[test]
	fn just_past_names_a_key_that_needs_no_successor() {
		// the unit key has no successor to name, and answering Top would swallow every key above it, so
		// the edge has to carry the exclusivity itself to end a range on the greatest key
		assert_eq!(Edge::just_past(&()), Edge::AfterKey(()));
		assert!(Edge::just_past(&()).covers(&()));
	}

	#[test]
	fn just_past_covers_its_own_key_and_nothing_after_it() {
		// this is the half open end of the single key range, so it must admit the key and refuse the
		// very next one, otherwise shrink_key would clear a neighbour it never named
		let key = EncodedKey::new([0x01, 0x02]);
		let end = Edge::just_past(&key);
		assert!(end.covers(&key));
		assert!(!end.covers(&key.successor().unwrap()));
		assert!(!end.covers(&EncodedKey::new([0x02])));
		assert_eq!(end, Edge::Key(EncodedKey::new([0x01, 0x02, 0x00])));
	}
}
