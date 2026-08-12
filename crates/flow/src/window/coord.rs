// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::{
	state::seal::coord::{Coord, IsZero},
	window::span::Slot,
};

pub trait TimeStamped {
	fn row_time(&self) -> DateTime;
}

impl TimeStamped for DateTime {
	fn row_time(&self) -> DateTime {
		*self
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventCoord(DateTime);

impl EventCoord {
	pub fn of(row: &impl TimeStamped) -> Self {
		Self(row.row_time())
	}

	pub fn at(self) -> DateTime {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RowSpan {
	rows: u64,
}

impl RowSpan {
	pub const ZERO: Self = Self {
		rows: 0,
	};

	pub fn of(rows: u64) -> Self {
		Self {
			rows,
		}
	}

	pub fn rows(self) -> u64 {
		self.rows
	}
}

impl IsZero for RowSpan {
	#[inline]
	fn is_zero(&self) -> bool {
		self.rows == 0
	}
}

#[operator_state]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrdinalCoord {
	ordinal: u64,
}

impl OrdinalCoord {
	pub fn from_arrival_counter(ordinal: u64) -> Self {
		Self {
			ordinal,
		}
	}

	pub fn from_row_number(row_number: RowNumber) -> Self {
		Self {
			ordinal: row_number.0,
		}
	}

	pub fn value(self) -> u64 {
		self.ordinal
	}
}

impl HeapSize for OrdinalCoord {
	fn heap_size(&self) -> usize {
		0
	}
}

impl Coord for OrdinalCoord {
	type Span = RowSpan;

	const MAX: Self = Self {
		ordinal: u64::MAX,
	};

	fn saturating_sub_span(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal.saturating_sub(span.rows),
		}
	}

	fn checked_sub_span(self, span: RowSpan) -> Option<Self> {
		self.ordinal.checked_sub(span.rows).map(|ordinal| Self {
			ordinal,
		})
	}

	fn add_span(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal + span.rows,
		}
	}

	fn floor_to(self, span: RowSpan) -> Self {
		Self {
			ordinal: self.ordinal - (self.ordinal % span.rows),
		}
	}

	fn span_since(self, earlier: Self) -> RowSpan {
		RowSpan {
			rows: self.ordinal - earlier.ordinal,
		}
	}

	fn to_order(self) -> u64 {
		self.ordinal
	}

	fn from_order(order: u64) -> Self {
		Self {
			ordinal: order,
		}
	}

	fn span_millis(_span: RowSpan) -> Option<u64> {
		None
	}
}

impl Slot for OrdinalCoord {
	type Coord = OrdinalCoord;

	fn order_key(&self) -> OrdinalCoord {
		*self
	}

	fn from_order_key(coord: OrdinalCoord) -> Self {
		coord
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::operator::encode;

	use super::*;

	struct Row {
		time: DateTime,
		other_column: DateTime,
	}

	impl TimeStamped for Row {
		fn row_time(&self) -> DateTime {
			self.time
		}
	}

	#[test]
	fn an_event_coordinate_can_only_come_from_the_row_time() {
		// `of` is the only constructor, so a coordinate cannot come from a data column, a config
		// value or a clock read. The row's second DateTime is what an operator would be tempted to
		// bucket by, keying the window on something the substrate can neither see nor seal against.
		let row = Row {
			time: DateTime::from_millis(5_000),
			other_column: DateTime::from_millis(9_999),
		};

		assert_eq!(EventCoord::of(&row).at(), DateTime::from_millis(5_000));
		assert_ne!(EventCoord::of(&row).at(), row.other_column);
	}

	#[test]
	fn event_coordinates_order_by_instant() {
		// Ordering is what the seal ledger and the admissible-span comparison are built
		// on, so it must be the instant's order and nothing else.
		let early = EventCoord::of(&DateTime::from_millis(1));
		let late = EventCoord::of(&DateTime::from_millis(2));

		assert!(early < late);
	}

	#[test]
	fn an_ordinal_encodes_to_the_same_bytes_as_the_bare_count_it_replaced() {
		// A changed persisted layout is silent: stored buffer keys get reinterpreted, not rejected.
		let value = 0x0123_4567_89AB_CDEFu64;

		let wrapped = encode(&OrdinalCoord::from_arrival_counter(value), DateTime::EPOCH).expect("encode");
		let bare = encode(&value, DateTime::EPOCH).expect("encode");

		assert_eq!(wrapped.body(), bare.body(), "the newtype changed the persisted layout");
	}

	#[test]
	fn ordinal_arithmetic_counts_rows_and_refuses_to_answer_in_milliseconds() {
		// A span here is rows, not milliseconds. span_millis answering Some would let a row count
		// reach the seal horizon as a duration.
		let coord = OrdinalCoord::from_arrival_counter(100);

		assert_eq!(coord.saturating_sub_span(RowSpan::of(64)), OrdinalCoord::from_arrival_counter(36));
		assert_eq!(coord.add_span(RowSpan::of(5)), OrdinalCoord::from_arrival_counter(105));
		assert_eq!(coord.span_since(OrdinalCoord::from_arrival_counter(60)), RowSpan::of(40));
		assert_eq!(<OrdinalCoord as Coord>::span_millis(RowSpan::of(64)), None);
	}

	#[test]
	fn an_ordinal_below_its_own_span_has_no_earlier_coordinate_rather_than_wrapping() {
		// Wrapping below zero lands near u64::MAX and evicts the whole buffer on the first pass.
		let coord = OrdinalCoord::from_arrival_counter(10);

		assert_eq!(coord.checked_sub_span(RowSpan::of(11)), None);
		assert_eq!(coord.checked_sub_span(RowSpan::of(10)), Some(OrdinalCoord::from_arrival_counter(0)));
		assert_eq!(coord.saturating_sub_span(RowSpan::of(11)), OrdinalCoord::from_arrival_counter(0));
	}

	#[test]
	fn both_ordinal_sources_produce_the_same_domain() {
		// An ordinal can be minted from a per-group arrival counter or from a RowNumber, and both
		// must land in one domain type or each count kind would need its own driver.
		let minted = OrdinalCoord::from_arrival_counter(7);
		let from_row = OrdinalCoord::from_row_number(RowNumber(7));

		assert_eq!(minted, from_row);
		assert_eq!(minted.value(), 7);
	}
}
