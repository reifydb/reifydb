// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use reifydb_codec::key::encoded::EncodedKey;
pub use reifydb_macro::Key;

use crate::metrics::heap::HeapSize;

pub mod direction;
pub mod layout;
pub mod range;

pub trait Key: Clone + Ord + Hash + Debug + HeapSize + Send + Sync + 'static {
	fn low() -> Self;

	fn successor(&self) -> Option<Self>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExclusiveUpperEnd<K = MultiKey> {
	Key(K),
	Top,
}

impl<K> ExclusiveUpperEnd<K> {
	pub fn is_top(&self) -> bool {
		matches!(self, ExclusiveUpperEnd::Top)
	}

	pub fn key(&self) -> Option<&K> {
		match self {
			ExclusiveUpperEnd::Key(key) => Some(key),
			ExclusiveUpperEnd::Top => None,
		}
	}
}

impl<K: Key> ExclusiveUpperEnd<K> {
	pub fn just_past(key: &K) -> Self {
		match key.successor() {
			Some(next) => ExclusiveUpperEnd::Key(next),
			None => ExclusiveUpperEnd::Top,
		}
	}
}

impl ExclusiveUpperEnd<MultiKey> {
	pub fn of(key: impl AsRef<[u8]>) -> Self {
		ExclusiveUpperEnd::Key(EncodedKey::new(key))
	}
}

impl<K: Ord> ExclusiveUpperEnd<K> {
	pub fn cmp_key(&self, key: &K) -> Ordering {
		match self {
			ExclusiveUpperEnd::Key(edge) => edge.cmp(key),
			ExclusiveUpperEnd::Top => Ordering::Greater,
		}
	}

	pub fn covers(&self, key: &K) -> bool {
		self.cmp_key(key) == Ordering::Greater
	}

	pub fn min(self, other: Self) -> Self {
		match (&self, &other) {
			(ExclusiveUpperEnd::Top, _) => other,
			(_, ExclusiveUpperEnd::Top) => self,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => {
				if left <= right {
					self
				} else {
					other
				}
			}
		}
	}

	pub fn max(self, other: Self) -> Self {
		match (&self, &other) {
			(ExclusiveUpperEnd::Top, _) | (_, ExclusiveUpperEnd::Top) => ExclusiveUpperEnd::Top,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => {
				if left >= right {
					self
				} else {
					other
				}
			}
		}
	}
}

impl<K: Ord> PartialOrd for ExclusiveUpperEnd<K> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<K: Ord> Ord for ExclusiveUpperEnd<K> {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(ExclusiveUpperEnd::Top, ExclusiveUpperEnd::Top) => Ordering::Equal,
			(ExclusiveUpperEnd::Top, ExclusiveUpperEnd::Key(_)) => Ordering::Greater,
			(ExclusiveUpperEnd::Key(_), ExclusiveUpperEnd::Top) => Ordering::Less,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => left.cmp(right),
		}
	}
}

pub type MultiKey = EncodedKey;

impl Key for () {
	fn low() -> Self {}

	fn successor(&self) -> Option<Self> {
		None
	}
}

impl Key for EncodedKey {
	fn low() -> Self {
		EncodedKey::new([])
	}

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

	use super::{ExclusiveUpperEnd, Key, MultiKey};

	#[test]
	fn unit_key_has_no_successor() {
		// a group only keyspace subtracts its whole key, so the empty key must report the top of its space
		assert_eq!(<() as Key>::low(), ());
		assert_eq!(<() as Key>::successor(&()), None);
	}

	#[test]
	fn encoded_key_low_is_empty() {
		assert_eq!(<MultiKey as Key>::low().as_slice(), &[] as &[u8]);
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
		let end: ExclusiveUpperEnd<MultiKey> = ExclusiveUpperEnd::Key(EncodedKey::new([0x01]));
		assert_ne!(end, ExclusiveUpperEnd::Top);
		assert_eq!(end.clone(), end);
	}

	#[test]
	fn just_past_promotes_a_key_with_no_successor_to_the_top() {
		// successor became partial when keys stopped being byte strings; mapping None to anything but
		// Top would drop the greatest key out of every range that was meant to include it
		assert_eq!(ExclusiveUpperEnd::just_past(&()), ExclusiveUpperEnd::Top);
		assert!(ExclusiveUpperEnd::just_past(&()).covers(&()));
	}

	#[test]
	fn just_past_covers_its_own_key_and_nothing_after_it() {
		// this is the half open end of the single key range, so it must admit the key and refuse the
		// very next one, otherwise shrink_key would clear a neighbour it never named
		let key = EncodedKey::new([0x01, 0x02]);
		let end = ExclusiveUpperEnd::just_past(&key);
		assert!(end.covers(&key));
		assert!(!end.covers(&key.successor().unwrap()));
		assert!(!end.covers(&EncodedKey::new([0x02])));
		assert_eq!(end, ExclusiveUpperEnd::Key(EncodedKey::new([0x01, 0x02, 0x00])));
	}
}
