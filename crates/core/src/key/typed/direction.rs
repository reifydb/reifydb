// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use reifydb_codec::row::shape::fingerprint::RowShapeFingerprint;
use reifydb_value::{
	util::hash::{Hash64, Hash128},
	value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
};

use crate::{
	key::{
		operator::state::GroupId,
		typed::{
			TypedKey,
			layout::{KeyColumnType, KeyValue},
		},
	},
	metrics::heap::HeapSize,
	state::{join::ContentVersion, timer::TimerKind},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Direction {
	Asc,
	Desc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Asc<T>(pub T);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Desc<T>(pub T);

impl<T: Ord> Ord for Asc<T> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.cmp(&other.0)
	}
}

impl<T: Ord> PartialOrd for Asc<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<T: Ord> Ord for Desc<T> {
	fn cmp(&self, other: &Self) -> Ordering {
		other.0.cmp(&self.0)
	}
}

impl<T: Ord> PartialOrd for Desc<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<T: HeapSize> HeapSize for Asc<T> {
	fn heap_size(&self) -> usize {
		self.0.heap_size()
	}
}

impl<T: HeapSize> HeapSize for Desc<T> {
	fn heap_size(&self) -> usize {
		self.0.heap_size()
	}
}

pub trait KeyScalar: Clone + Ord + Hash + Debug + HeapSize + Send + Sync + 'static {
	const MIN: Self;
	const MAX: Self;
	const COLUMN_TYPE: KeyColumnType;

	fn successor(&self) -> Option<Self>;

	fn predecessor(&self) -> Option<Self>;

	fn to_key_value(&self) -> KeyValue;

	fn from_key_value(value: KeyValue) -> Option<Self>;
}

impl KeyScalar for u8 {
	const MIN: Self = u8::MIN;
	const MAX: Self = u8::MAX;
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U8;

	fn successor(&self) -> Option<Self> {
		self.checked_add(1)
	}

	fn predecessor(&self) -> Option<Self> {
		self.checked_sub(1)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U8(*self)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U8(v) => Some(v),
			_ => None,
		}
	}
}

impl KeyScalar for u64 {
	const MIN: Self = u64::MIN;
	const MAX: Self = u64::MAX;
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U64;

	fn successor(&self) -> Option<Self> {
		self.checked_add(1)
	}

	fn predecessor(&self) -> Option<Self> {
		self.checked_sub(1)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U64(*self)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U64(v) => Some(v),
			_ => None,
		}
	}
}

impl KeyScalar for [u8; 16] {
	const MIN: Self = [0x00; 16];
	const MAX: Self = [0xff; 16];
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::Blob16;

	fn successor(&self) -> Option<Self> {
		let mut out = *self;
		for byte in out.iter_mut().rev() {
			match byte.checked_add(1) {
				Some(next) => {
					*byte = next;
					return Some(out);
				}
				None => *byte = 0x00,
			}
		}
		None
	}

	fn predecessor(&self) -> Option<Self> {
		let mut out = *self;
		for byte in out.iter_mut().rev() {
			match byte.checked_sub(1) {
				Some(previous) => {
					*byte = previous;
					return Some(out);
				}
				None => *byte = 0xff,
			}
		}
		None
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::Blob16(*self)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::Blob16(v) => Some(v),
			_ => None,
		}
	}
}

impl KeyScalar for GroupId {
	const MIN: Self = GroupId::MIN;
	const MAX: Self = GroupId::MAX;
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::Blob24;

	fn successor(&self) -> Option<Self> {
		GroupId::successor(self)
	}

	fn predecessor(&self) -> Option<Self> {
		GroupId::predecessor(self)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::Blob24(*self.as_bytes())
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::Blob24(v) => Some(GroupId::from_bytes(v)),
			_ => None,
		}
	}
}

impl KeyScalar for RowNumber {
	const MIN: Self = RowNumber(u64::MIN);
	const MAX: Self = RowNumber(u64::MAX);
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U64;

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(RowNumber)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(RowNumber)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U64(self.0)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U64(v) => Some(RowNumber(v)),
			_ => None,
		}
	}
}

impl KeyScalar for ContentVersion {
	const MIN: Self = ContentVersion(u64::MIN);
	const MAX: Self = ContentVersion(u64::MAX);
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U64;

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(ContentVersion)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(ContentVersion)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U64(self.0)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U64(v) => Some(ContentVersion(v)),
			_ => None,
		}
	}
}

impl KeyScalar for RowShapeFingerprint {
	const MIN: Self = RowShapeFingerprint(Hash64(u64::MIN));
	const MAX: Self = RowShapeFingerprint(Hash64(u64::MAX));
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U64;

	fn successor(&self) -> Option<Self> {
		self.0.0.checked_add(1).map(|next| RowShapeFingerprint(Hash64(next)))
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.0.checked_sub(1).map(|previous| RowShapeFingerprint(Hash64(previous)))
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U64(self.0.0)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U64(v) => Some(RowShapeFingerprint(Hash64(v))),
			_ => None,
		}
	}
}

impl KeyScalar for Hash128 {
	const MIN: Self = Hash128(u128::MIN);
	const MAX: Self = Hash128(u128::MAX);
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::Blob16;

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(Hash128)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(Hash128)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::Blob16(self.0.to_be_bytes())
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::Blob16(v) => Some(Hash128(u128::from_be_bytes(v))),
			_ => None,
		}
	}
}

impl KeyScalar for Partition {
	const MIN: Self = Partition(u128::MIN);
	const MAX: Self = Partition(u128::MAX);
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::Blob16;

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(Partition)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(Partition)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::Blob16(self.0.to_be_bytes())
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::Blob16(v) => Some(Partition(u128::from_be_bytes(v))),
			_ => None,
		}
	}
}

impl KeyScalar for DateTime {
	const MIN: Self = DateTime::EPOCH;
	const MAX: Self = DateTime::MAX;
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U64;

	fn successor(&self) -> Option<Self> {
		self.to_bits().checked_add(1).map(DateTime::from_bits)
	}

	fn predecessor(&self) -> Option<Self> {
		self.to_bits().checked_sub(1).map(DateTime::from_bits)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U64(self.to_bits())
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U64(v) => Some(DateTime::from_bits(v)),
			_ => None,
		}
	}
}

impl KeyScalar for TimerKind {
	const MIN: Self = TimerKind::Seal;
	const MAX: Self = TimerKind::Maintenance;
	const COLUMN_TYPE: KeyColumnType = KeyColumnType::U8;

	fn successor(&self) -> Option<Self> {
		TimerKind::from_u8(*self as u8 + 1)
	}

	fn predecessor(&self) -> Option<Self> {
		(*self as u8).checked_sub(1).and_then(TimerKind::from_u8)
	}
	fn to_key_value(&self) -> KeyValue {
		KeyValue::U8(*self as u8)
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		match value {
			KeyValue::U8(v) => TimerKind::from_u8(v),
			_ => None,
		}
	}
}

impl<T: KeyScalar> TypedKey for Asc<T> {
	fn low() -> Self {
		Asc(T::MIN)
	}

	fn successor(&self) -> Option<Self> {
		self.0.successor().map(Asc)
	}
}

impl<T: KeyScalar> TypedKey for Desc<T> {
	fn low() -> Self {
		Desc(T::MAX)
	}

	fn successor(&self) -> Option<Self> {
		self.0.predecessor().map(Desc)
	}
}

mod sealed {
	pub trait Sealed {}
}

pub trait KeyField: sealed::Sealed {
	const DIRECTION: Direction;

	fn to_key_value(&self) -> KeyValue;

	fn from_key_value(value: KeyValue) -> Option<Self>
	where
		Self: Sized;
}

impl<T> sealed::Sealed for Asc<T> {}

impl<T> sealed::Sealed for Desc<T> {}

impl<T: KeyScalar> KeyField for Asc<T> {
	const DIRECTION: Direction = Direction::Asc;

	fn to_key_value(&self) -> KeyValue {
		self.0.to_key_value()
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		T::from_key_value(value).map(Asc)
	}
}

impl<T: KeyScalar> KeyField for Desc<T> {
	const DIRECTION: Direction = Direction::Desc;

	fn to_key_value(&self) -> KeyValue {
		self.0.to_key_value()
	}

	fn from_key_value(value: KeyValue) -> Option<Self> {
		T::from_key_value(value).map(Desc)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::shape::fingerprint::RowShapeFingerprint;
	use reifydb_value::{
		util::hash::{Hash64, Hash128},
		value::{datetime::DateTime, partition::Partition, row_number::RowNumber},
	};

	use super::{Asc, Desc, Direction, KeyField, KeyScalar};
	use crate::{
		key::{operator::state::GroupId, typed::TypedKey},
		metrics::heap::HeapSize,
		state::{join::ContentVersion, timer::TimerKind},
	};

	fn walks_its_whole_domain<T: KeyScalar>(low: T, high: T) {
		// every scalar must agree that MIN and MAX are the ends and that stepping is immediate, or an
		// exclusive upper end silently drops the row that sorts between a key and its successor
		assert_eq!(T::MIN.predecessor(), None, "MIN must have no predecessor");
		assert_eq!(T::MAX.successor(), None, "MAX must have no successor");
		assert!(T::MIN <= low && high <= T::MAX, "the probes must lie inside the declared bounds");
		assert!(low < high);
		let next = low.successor().expect("a value below MAX must have a successor");
		assert!(next > low);
		assert_eq!(next.predecessor(), Some(low), "predecessor must undo successor exactly");
	}

	#[test]
	fn content_version_walks_its_whole_domain() {
		walks_its_whole_domain(ContentVersion(7), ContentVersion(9));
	}

	#[test]
	fn row_shape_fingerprint_walks_its_whole_domain() {
		walks_its_whole_domain(RowShapeFingerprint(Hash64(7)), RowShapeFingerprint(Hash64(9)));
	}

	#[test]
	fn hash128_walks_its_whole_domain() {
		walks_its_whole_domain(Hash128(7), Hash128(9));
	}

	#[test]
	fn partition_walks_its_whole_domain() {
		walks_its_whole_domain(Partition(7), Partition(9));
	}

	#[test]
	fn datetime_walks_its_whole_domain() {
		walks_its_whole_domain(DateTime::from_bits(7), DateTime::from_bits(9));
	}

	#[test]
	fn timer_kind_walks_its_whole_domain() {
		walks_its_whole_domain(TimerKind::Grace, TimerKind::RowTtl);
	}

	#[test]
	fn timer_kind_orders_by_its_repr_discriminant() {
		// the key column stores the discriminant byte, so an Ord that disagreed with `as u8` would sort
		// the wheel index differently in memory than in sqlite
		let mut kinds = vec![TimerKind::Maintenance, TimerKind::Seal, TimerKind::RowTtl, TimerKind::Grace];
		kinds.sort();
		let discriminants: Vec<u8> = kinds.iter().map(|kind| *kind as u8).collect();
		assert_eq!(discriminants, vec![0, 1, 2, 3]);
	}

	#[test]
	fn timer_kind_successor_stops_at_the_last_declared_variant() {
		// from_u8 is what bounds the walk; a wider MAX would hand out a variant that does not exist
		assert_eq!(TimerKind::Maintenance.successor(), None);
		assert_eq!(TimerKind::Seal.predecessor(), None);
		assert_eq!(TimerKind::Seal.successor(), Some(TimerKind::Grace));
	}

	#[test]
	fn datetime_orders_by_the_bits_the_key_column_stores() {
		// the column is written as to_bits(), so Ord disagreeing with bit order would make a wheel scan
		// return rows the range never contained
		assert!(DateTime::from_bits(1) < DateTime::from_bits(2));
		assert_eq!(<Desc<DateTime> as TypedKey>::low(), Desc(DateTime::MAX));
		assert_eq!(<Asc<DateTime> as TypedKey>::low(), Asc(DateTime::EPOCH));
	}

	#[test]
	fn desc_of_every_new_scalar_starts_at_its_maximum() {
		// Desc<T>::low() is T::MAX by definition; a scalar whose MAX is not its true top would start a
		// descending scan below the first row and skip it
		assert_eq!(<Desc<Hash128> as TypedKey>::low(), Desc(Hash128(u128::MAX)));
		assert_eq!(<Desc<Partition> as TypedKey>::low(), Desc(Partition(u128::MAX)));
		assert_eq!(<Desc<ContentVersion> as TypedKey>::low(), Desc(ContentVersion(u64::MAX)));
		assert_eq!(<Desc<TimerKind> as TypedKey>::low(), Desc(TimerKind::Maintenance));
	}

	#[test]
	fn asc_orders_like_the_inner_value() {
		// Asc is the identity wrapper: if it ever flipped, every ascending keyspace would scan backwards
		assert!(Asc(1) < Asc(2));
		assert!(Asc(2) > Asc(1));
		assert_eq!(Asc(1), Asc(1));
	}

	#[test]
	fn desc_flips_the_inner_value() {
		// the whole point of Desc: the derived ascending Ord must not leak through
		assert!(Desc(1) > Desc(2));
		assert!(Desc(2) < Desc(1));
		assert_eq!(Desc(1), Desc(1));
	}

	#[test]
	fn sorting_a_desc_column_yields_descending_inner_values() {
		let mut values = vec![Desc(3u64), Desc(1), Desc(2)];
		values.sort();
		assert_eq!(values, vec![Desc(3), Desc(2), Desc(1)]);
	}

	#[test]
	fn sorting_an_asc_column_yields_ascending_inner_values() {
		let mut values = vec![Asc(3u64), Asc(1), Asc(2)];
		values.sort();
		assert_eq!(values, vec![Asc(1), Asc(2), Asc(3)]);
	}

	#[test]
	fn direction_is_readable_from_the_wrapper_type() {
		// the derive reads the column direction off this const, so it must agree with the Ord impls above
		assert_eq!(<Asc<u64> as KeyField>::DIRECTION, Direction::Asc);
		assert_eq!(<Desc<u64> as KeyField>::DIRECTION, Direction::Desc);
	}

	#[test]
	fn heap_size_delegates_to_the_inner_value() {
		// a wrapper that charged its own bytes would double count the memory budget
		assert_eq!(Asc(1u64).heap_size(), 0);
		assert_eq!(Desc(1u64).heap_size(), 0);
		assert_eq!(Asc(vec![1u8, 2, 3]).heap_size(), vec![1u8, 2, 3].heap_size());
	}

	#[test]
	fn asc_runs_the_inner_domain_forwards() {
		assert_eq!(<Asc<u64> as TypedKey>::low(), Asc(u64::MIN));
		assert_eq!(Asc(5u64).successor(), Some(Asc(6)));
		assert_eq!(Asc(u64::MAX).successor(), None);
	}

	#[test]
	fn desc_low_is_the_inner_maximum() {
		// Desc runs the order backwards, so the first key of a descending column is the largest value;
		// low() returning the minimum would place the scan start past the end of the keyspace
		assert_eq!(<Desc<u64> as TypedKey>::low(), Desc(u64::MAX));
		assert_eq!(<Desc<u8> as TypedKey>::low(), Desc(u8::MAX));
	}

	#[test]
	fn desc_successor_is_the_inner_predecessor() {
		// the next key in descending order is one below, not one above; an ascending successor here walks
		// away from the rows the scan is meant to reach
		assert_eq!(Desc(5u64).successor(), Some(Desc(4)));
		assert_eq!(Desc(1u64).successor(), Some(Desc(0)));
	}

	#[test]
	fn desc_successor_runs_out_at_the_inner_minimum() {
		// zero is the top of a descending u64 space, so it must report none rather than wrapping
		assert_eq!(Desc(0u64).successor(), None);
		assert_eq!(Desc(0u8).successor(), None);
	}

	#[test]
	fn desc_low_and_successor_walk_the_whole_order() {
		// low() then repeated successor() must visit a descending column in its own sort order
		let mut walk = vec![<Desc<u8> as TypedKey>::low()];
		while let Some(next) = walk.last().unwrap().successor() {
			walk.push(next);
			if walk.len() > 4 {
				break;
			}
		}
		assert_eq!(walk, vec![Desc(255u8), Desc(254), Desc(253), Desc(252), Desc(251)]);
		assert!(walk.windows(2).all(|pair| pair[0] < pair[1]));
	}

	#[test]
	fn byte_array_scalars_carry_across_bytes() {
		// the group column is sixteen bytes big endian, so its successor has to carry, not increment one byte
		assert_eq!([0x00u8; 16].successor().unwrap()[15], 0x01);
		let mut carried = [0x00u8; 16];
		carried[15] = 0xff;
		let next = carried.successor().unwrap();
		assert_eq!(next[14], 0x01);
		assert_eq!(next[15], 0x00);
		assert_eq!(<[u8; 16] as KeyScalar>::MAX.successor(), None);
		assert_eq!(<[u8; 16] as KeyScalar>::MIN.predecessor(), None);
	}

	#[test]
	fn group_and_row_number_scalars_bound_their_domains() {
		assert_eq!(<GroupId as KeyScalar>::MIN, GroupId::ROOT);
		assert_eq!(GroupId::MAX.successor(), None);
		assert_eq!(<Desc<GroupId> as TypedKey>::low(), Desc(GroupId::MAX));
		assert_eq!(<RowNumber as KeyScalar>::MAX, RowNumber(u64::MAX));
		assert_eq!(RowNumber(0).predecessor(), None);
		assert_eq!(<Desc<RowNumber> as TypedKey>::low(), Desc(RowNumber(u64::MAX)));
	}
}
