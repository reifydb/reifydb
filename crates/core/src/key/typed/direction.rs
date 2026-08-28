// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use reifydb_value::value::row_number::RowNumber;

use crate::{
	key::{operator_state::GroupId, typed::Key},
	metrics::heap::HeapSize,
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

	fn successor(&self) -> Option<Self>;

	fn predecessor(&self) -> Option<Self>;
}

impl KeyScalar for u8 {
	const MIN: Self = u8::MIN;
	const MAX: Self = u8::MAX;

	fn successor(&self) -> Option<Self> {
		self.checked_add(1)
	}

	fn predecessor(&self) -> Option<Self> {
		self.checked_sub(1)
	}
}

impl KeyScalar for u64 {
	const MIN: Self = u64::MIN;
	const MAX: Self = u64::MAX;

	fn successor(&self) -> Option<Self> {
		self.checked_add(1)
	}

	fn predecessor(&self) -> Option<Self> {
		self.checked_sub(1)
	}
}

impl KeyScalar for [u8; 16] {
	const MIN: Self = [0x00; 16];
	const MAX: Self = [0xff; 16];

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
}

impl KeyScalar for GroupId {
	const MIN: Self = GroupId(u128::MIN);
	const MAX: Self = GroupId(u128::MAX);

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(GroupId)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(GroupId)
	}
}

impl KeyScalar for RowNumber {
	const MIN: Self = RowNumber(u64::MIN);
	const MAX: Self = RowNumber(u64::MAX);

	fn successor(&self) -> Option<Self> {
		self.0.checked_add(1).map(RowNumber)
	}

	fn predecessor(&self) -> Option<Self> {
		self.0.checked_sub(1).map(RowNumber)
	}
}

impl<T: KeyScalar> Key for Asc<T> {
	fn low() -> Self {
		Asc(T::MIN)
	}

	fn successor(&self) -> Option<Self> {
		self.0.successor().map(Asc)
	}
}

impl<T: KeyScalar> Key for Desc<T> {
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
}

impl<T> sealed::Sealed for Asc<T> {}

impl<T> sealed::Sealed for Desc<T> {}

impl<T> KeyField for Asc<T> {
	const DIRECTION: Direction = Direction::Asc;
}

impl<T> KeyField for Desc<T> {
	const DIRECTION: Direction = Direction::Desc;
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::row_number::RowNumber;

	use super::{Asc, Desc, Direction, KeyField, KeyScalar};
	use crate::{
		key::{operator_state::GroupId, typed::Key},
		metrics::heap::HeapSize,
	};

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
		assert_eq!(<Asc<u64> as Key>::low(), Asc(u64::MIN));
		assert_eq!(Asc(5u64).successor(), Some(Asc(6)));
		assert_eq!(Asc(u64::MAX).successor(), None);
	}

	#[test]
	fn desc_low_is_the_inner_maximum() {
		// Desc runs the order backwards, so the first key of a descending column is the largest value;
		// low() returning the minimum would place the scan start past the end of the keyspace
		assert_eq!(<Desc<u64> as Key>::low(), Desc(u64::MAX));
		assert_eq!(<Desc<u8> as Key>::low(), Desc(u8::MAX));
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
		let mut walk = vec![<Desc<u8> as Key>::low()];
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
		assert_eq!(<GroupId as KeyScalar>::MIN, GroupId(0));
		assert_eq!(GroupId(u128::MAX).successor(), None);
		assert_eq!(<Desc<GroupId> as Key>::low(), Desc(GroupId(u128::MAX)));
		assert_eq!(<RowNumber as KeyScalar>::MAX, RowNumber(u64::MAX));
		assert_eq!(RowNumber(0).predecessor(), None);
		assert_eq!(<Desc<RowNumber> as Key>::low(), Desc(RowNumber(u64::MAX)));
	}
}
