// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{util::hash::xxh3_128, value::datetime::DateTime};

use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{
			Key,
			direction::{Asc, Direction, KeyField},
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue},
		},
	},
	metrics::heap::HeapSize,
	state::timer::TimerKind,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct TimerWheelKey {
	pub due: Asc<DateTime>,
	pub kind: Asc<TimerKind>,
	pub id: Asc<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
pub struct TimerIndexKey {
	pub kind: Asc<TimerKind>,
	pub id: Asc<[u8; 16]>,
}

pub fn timer_id(bytes: &[u8]) -> [u8; 16] {
	xxh3_128(bytes).0.to_be_bytes()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerWheel;

impl Keyspace for TimerWheel {
	const ID: KeyspaceId = KeyspaceId::TIMER_WHEEL;
	const NAME: &'static str = "TIMER_WHEEL";
	const CACHE: CacheTiers = CacheTiers::Range;

	type Key = TimerWheelKey;
	type Suffix = TimerWheelKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimerIndex;

impl Keyspace for TimerIndex {
	const ID: KeyspaceId = KeyspaceId::TIMER_INDEX;
	const NAME: &'static str = "TIMER_INDEX";
	const CACHE: CacheTiers = CacheTiers::Both;

	type Key = TimerIndexKey;
	type Suffix = TimerIndexKey;

	fn split(key: &Self::Key) -> (GroupId, Self::Suffix) {
		(GroupId::ROOT, *key)
	}

	fn join(_group: GroupId, suffix: Self::Suffix) -> Self::Key {
		suffix
	}
}

#[cfg(test)]
mod tests {
	use super::{TimerIndexKey, TimerWheelKey, timer_id};
	use crate::key::typed::{
		direction::{Asc, Direction},
		layout::{KeyColumn, KeyColumnType, KeyLayout},
	};

	#[test]
	fn a_timer_id_of_any_length_narrows_to_the_key_width() {
		// two built-in producers exceed sixteen bytes and one of them serialises user supplied values,
		// so a wider id must be folded here rather than truncated at the key boundary
		for len in [0usize, 1, 16, 25, 4096] {
			let bytes = vec![0xABu8; len];
			assert_eq!(timer_id(&bytes).len(), 16, "an id of {len} bytes must still key on sixteen");
		}
	}

	#[test]
	fn distinct_timer_ids_stay_distinct_after_narrowing() {
		// the wheel orders by time first and the id is identity, not sort order, so a collision here
		// silently makes two timers the same row
		let long = vec![0x11u8; 25];
		let mut other = long.clone();
		other[24] = 0x12;
		assert_ne!(timer_id(&long), timer_id(&other));
		assert_ne!(timer_id(b""), timer_id(b"\0"));
	}

	#[test]
	fn a_timer_id_is_stable_across_calls() {
		// the id is written once and looked up later; a per process seed would lose every armed timer
		assert_eq!(timer_id(b"seal:window:7"), timer_id(b"seal:window:7"));
	}

	#[test]
	fn timer_ids_order_as_the_unsigned_integers_they_hash_to() {
		// R14: the big endian array is what sqlite stores and what derived Ord compares, so the two must
		// agree or the in memory wheel and the table would walk the index in different orders
		let low = 7u128.to_be_bytes();
		let high = 8u128.to_be_bytes();
		assert!(low < high);
		assert!(Asc(low) < Asc(high));
	}

	#[test]
	fn the_wheel_leads_on_due_time_and_the_index_leads_on_kind() {
		// the wheel exists to answer "what is due next" in one forward scan, so due time must be the
		// leading column; the index answers "where is this timer" and leads on kind instead
		assert_eq!(TimerWheelKey::COLUMNS[0].name, "due");
		assert_eq!(TimerWheelKey::COLUMNS[0].direction, Direction::Asc);
		assert_eq!(TimerWheelKey::COLUMNS[2].ty, KeyColumnType::Blob16);
		assert_eq!(TimerIndexKey::COLUMNS[0].name, "kind");
		assert_eq!(
			TimerIndexKey::COLUMNS[1],
			KeyColumn {
				name: "id",
				ty: KeyColumnType::Blob16,
				direction: Direction::Asc,
			}
		);
	}
}
