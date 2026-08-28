// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::key::encoded::EncodedKey;
pub use reifydb_macro::Key;

use crate::metrics::heap::HeapSize;

pub mod direction;
pub mod keyspace;
pub mod layout;

pub trait Key: Clone + Ord + Hash + Debug + HeapSize + Send + Sync + 'static {
	fn low() -> Self;

	fn successor(&self) -> Option<Self>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExclusiveUpperEnd<K> {
	Key(K),
	Top,
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
}
