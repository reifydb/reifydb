// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::key::typed::{
	Key,
	direction::{Asc, Desc, Direction, KeyScalar},
};

pub trait KeyLayout: Key {
	const COLUMNS: &'static [KeyColumn];

	fn key_values(&self) -> Vec<KeyValue>;

	fn from_key_values(values: &[KeyValue]) -> Option<Self>
	where
		Self: Sized;
}

impl KeyLayout for () {
	const COLUMNS: &'static [KeyColumn] = &[];

	fn key_values(&self) -> Vec<KeyValue> {
		Vec::new()
	}

	fn from_key_values(values: &[KeyValue]) -> Option<Self> {
		values.is_empty().then_some(())
	}
}

impl<T: KeyScalar> KeyLayout for Asc<T> {
	const COLUMNS: &'static [KeyColumn] = &[KeyColumn {
		name: "value",
		ty: T::COLUMN_TYPE,
		direction: Direction::Asc,
	}];

	fn key_values(&self) -> Vec<KeyValue> {
		vec![self.0.to_key_value()]
	}

	fn from_key_values(values: &[KeyValue]) -> Option<Self> {
		match values {
			[value] => T::from_key_value(*value).map(Asc),
			_ => None,
		}
	}
}

impl<T: KeyScalar> KeyLayout for Desc<T> {
	const COLUMNS: &'static [KeyColumn] = &[KeyColumn {
		name: "value",
		ty: T::COLUMN_TYPE,
		direction: Direction::Desc,
	}];

	fn key_values(&self) -> Vec<KeyValue> {
		vec![self.0.to_key_value()]
	}

	fn from_key_values(values: &[KeyValue]) -> Option<Self> {
		match values {
			[value] => T::from_key_value(*value).map(Desc),
			_ => None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct KeyColumn {
	pub name: &'static str,
	pub ty: KeyColumnType,
	pub direction: Direction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyColumnType {
	U8,
	U64,
	Blob16,
}

impl KeyColumnType {
	pub const fn width(self) -> usize {
		match self {
			Self::U8 => 1,
			Self::U64 => 8,
			Self::Blob16 => 16,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyValue {
	U8(u8),
	U64(u64),
	Blob16([u8; 16]),
}

impl KeyValue {
	pub fn column_type(&self) -> KeyColumnType {
		match self {
			Self::U8(_) => KeyColumnType::U8,
			Self::U64(_) => KeyColumnType::U64,
			Self::Blob16(_) => KeyColumnType::Blob16,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::row_number::RowNumber;

	use super::{KeyColumn, KeyColumnType, KeyLayout, KeyValue};
	use crate::{
		key::{
			operator::state::GroupId,
			typed::{
				Key,
				direction::{Asc, Desc, Direction, KeyField},
			},
		},
		metrics::heap::HeapSize,
	};

	#[derive(Clone, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
	struct ProbeKey {
		threshold: Desc<u64>,
		side: Asc<u8>,
	}

	#[derive(Clone, Debug, PartialEq, Eq, Hash, Key, HeapSize)]
	struct JoinLeftKey {
		group: Desc<GroupId>,
		row: Asc<RowNumber>,
	}

	fn probe(threshold: u64, side: u8) -> ProbeKey {
		ProbeKey {
			threshold: Desc(threshold),
			side: Asc(side),
		}
	}

	#[test]
	fn columns_follow_the_field_order() {
		// the table's composite primary key is emitted from this slice, so a reorder silently reindexes
		assert_eq!(ProbeKey::COLUMNS.len(), 2);
		assert_eq!(ProbeKey::COLUMNS[0].name, "threshold");
		assert_eq!(ProbeKey::COLUMNS[1].name, "side");
	}

	#[test]
	fn columns_carry_the_declared_direction_and_type() {
		assert_eq!(
			ProbeKey::COLUMNS[0],
			KeyColumn {
				name: "threshold",
				ty: KeyColumnType::U64,
				direction: Direction::Desc,
			}
		);
		assert_eq!(
			ProbeKey::COLUMNS[1],
			KeyColumn {
				name: "side",
				ty: KeyColumnType::U8,
				direction: Direction::Asc,
			}
		);
	}

	#[test]
	fn derived_ord_agrees_with_every_column_direction() {
		// this is the invariant the whole derive exists for: the sqlite index order is generated from
		// COLUMNS while the in memory tiers use Ord, so a disagreement here is a silent wrong answer
		for (index, column) in ProbeKey::COLUMNS.iter().enumerate() {
			let (low, high) = match index {
				0 => (probe(1, 0), probe(2, 0)),
				_ => (probe(1, 0), probe(1, 1)),
			};
			match column.direction {
				Direction::Asc => assert!(low < high, "column {} must sort ascending", column.name),
				Direction::Desc => assert!(low > high, "column {} must sort descending", column.name),
			}
		}
	}

	#[test]
	fn leading_column_outranks_the_trailing_one() {
		// field order is the sort order; if the trailing column ever won, group scans would interleave
		assert!(probe(2, 255) < probe(1, 0));
	}

	#[test]
	fn heap_size_of_a_fixed_width_key_is_zero() {
		assert_eq!(probe(1, 0).heap_size(), 0);
	}

	#[test]
	fn low_is_the_first_key_in_the_declared_order() {
		// the descending column starts at its maximum and the ascending one at its minimum, so a scan
		// that begins at low() begins at the true first row
		assert_eq!(ProbeKey::low(), probe(u64::MAX, u8::MIN));
	}

	#[test]
	fn successor_advances_the_rightmost_column_first() {
		assert_eq!(probe(9, 3).successor(), Some(probe(9, 4)));
	}

	#[test]
	fn successor_carries_left_and_resets_the_columns_to_its_right() {
		// the trailing ascending column runs out at 255, and carrying into a descending column means
		// stepping the threshold down, not up
		assert_eq!(probe(9, u8::MAX).successor(), Some(probe(8, u8::MIN)));
	}

	#[test]
	fn successor_is_none_only_when_every_column_overflows() {
		// the last key of the space is the descending column at its minimum and the ascending one at its
		// maximum; anything else must still have a successor
		assert_eq!(probe(0, u8::MAX).successor(), None);
		assert!(probe(0, 254).successor().is_some());
		assert!(probe(1, u8::MAX).successor().is_some());
	}

	#[test]
	fn successor_never_skips_a_key_in_the_derived_order() {
		// successor must be immediate: if anything sorts between a key and its successor, an exclusive
		// upper bound silently drops that row
		let mut walked = probe(u64::MAX, u8::MAX - 2);
		for _ in 0..4 {
			let next = walked.successor().unwrap();
			assert!(next > walked);
			walked = next;
		}
		assert_eq!(walked, probe(u64::MAX - 1, 1));
	}

	#[test]
	fn the_catalogue_join_key_shape_compiles_and_orders_both_ways() {
		// this is the plan's canonical key: it only builds if the derive's column mapping and the scalar
		// domain agree on RowNumber, and it is the shape five keyspaces share
		assert_eq!(
			JoinLeftKey::COLUMNS,
			&[
				KeyColumn {
					name: "group",
					ty: KeyColumnType::Blob16,
					direction: Direction::Desc,
				},
				KeyColumn {
					name: "row",
					ty: KeyColumnType::U64,
					direction: Direction::Asc,
				},
			]
		);
		assert_eq!(
			JoinLeftKey::low(),
			JoinLeftKey {
				group: Desc(GroupId(u128::MAX)),
				row: Asc(RowNumber(0)),
			}
		);
		let key = JoinLeftKey {
			group: Desc(GroupId(7)),
			row: Asc(RowNumber(u64::MAX)),
		};
		assert_eq!(
			key.successor(),
			Some(JoinLeftKey {
				group: Desc(GroupId(6)),
				row: Asc(RowNumber(0)),
			})
		);
	}
	#[test]
	fn key_values_round_trip_through_the_column_values() {
		// this is the only bridge between a typed key and its sqlite row; a lossy or reordered
		// conversion writes one key and reads back another, and both directions still look healthy
		let key = probe(0x0123_4567_89ab_cdef, 7);
		let values = key.key_values();
		assert_eq!(ProbeKey::from_key_values(&values), Some(key));

		let join = JoinLeftKey {
			group: Desc(GroupId(u128::MAX - 3)),
			row: Asc(RowNumber(42)),
		};
		assert_eq!(JoinLeftKey::from_key_values(&join.key_values()), Some(join));
	}

	#[test]
	fn key_values_are_positional_and_typed_like_the_columns() {
		// the binder walks COLUMNS and key_values() in lockstep, so a length or type disagreement binds
		// a u64 into a blob column and sqlite accepts it without complaint
		let key = probe(9, 3);
		let values = key.key_values();
		assert_eq!(values.len(), ProbeKey::COLUMNS.len());
		for (value, column) in values.iter().zip(ProbeKey::COLUMNS) {
			assert_eq!(value.column_type(), column.ty, "column {} binds the wrong width", column.name);
		}
		assert_eq!(values[0], KeyValue::U64(9));
		assert_eq!(values[1], KeyValue::U8(3));
	}

	#[test]
	fn a_descending_column_keeps_its_own_value_not_its_complement() {
		// Desc reverses Ord, not the stored bytes; complementing here would make the sqlite DESC index
		// and the in memory order disagree while every single key still round trips
		let key = probe(5, 0);
		assert_eq!(key.key_values()[0], KeyValue::U64(5));
	}

	#[test]
	fn a_value_list_of_the_wrong_length_is_refused() {
		// a short read means the row had fewer columns than the layout, which is a schema drift, and
		// filling the tail with defaults would silently address a different key
		assert_eq!(ProbeKey::from_key_values(&[KeyValue::U64(1)]), None);
		assert_eq!(ProbeKey::from_key_values(&[KeyValue::U64(1), KeyValue::U8(2), KeyValue::U8(3)]), None);
	}

	#[test]
	fn a_value_of_the_wrong_width_is_refused() {
		// sqlite is dynamically typed, so a blob in an integer column reaches here intact; accepting it
		// would decode a neighbouring key rather than fail the read
		assert_eq!(ProbeKey::from_key_values(&[KeyValue::Blob16([0; 16]), KeyValue::U8(2)]), None);
	}
}
