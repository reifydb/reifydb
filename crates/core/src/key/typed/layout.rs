// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::key::typed::{Key, direction::Direction};

pub trait KeyLayout: Key {
	const COLUMNS: &'static [KeyColumn];
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

#[cfg(test)]
mod tests {
	use reifydb_value::value::row_number::RowNumber;

	use super::{KeyColumn, KeyColumnType, KeyLayout};
	use crate::{
		key::{
			operator_state::GroupId,
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
}
