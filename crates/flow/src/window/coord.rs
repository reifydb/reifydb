// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use crate::window::span::WindowCoord;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrdinalCoord(u64);

impl OrdinalCoord {
	pub fn from_arrival_counter(ordinal: u64) -> Self {
		Self(ordinal)
	}

	pub fn from_row_number(row_number: RowNumber) -> Self {
		Self(row_number.0)
	}

	pub fn value(self) -> u64 {
		self.0
	}
}

pub trait WindowDomain {
	type Coord: WindowCoord;

	const SEALS_ON_TIMER: bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventTime;

impl WindowDomain for EventTime {
	type Coord = DateTime;

	const SEALS_ON_TIMER: bool = true;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ordinal;

impl WindowDomain for Ordinal {
	type Coord = u64;

	const SEALS_ON_TIMER: bool = false;
}

#[cfg(test)]
mod tests {
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
		// `of` is the only constructor, so a coordinate cannot be built from a data
		// column, a config value or a clock read. The row carries a second, more
		// "interesting" DateTime an operator would be tempted to bucket by; the window
		// would then key on something the substrate can neither see nor seal against.
		// The absence of an `at_instant(DateTime)` constructor is the thing under test.
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
	fn both_ordinal_sources_produce_the_same_domain() {
		// Count-based windows mint ordinals two different ways - a per-group
		// arrival counter for tumbling/sliding, the RowNumber for rolling - and the shell
		// must not care which. If these produced different types, every count kind would
		// need its own driver, which is the duplication this plan exists to delete.
		let minted = OrdinalCoord::from_arrival_counter(7);
		let from_row = OrdinalCoord::from_row_number(RowNumber(7));

		assert_eq!(minted, from_row);
		assert_eq!(minted.value(), 7);
	}

	#[test]
	fn only_the_event_time_domain_seals_on_a_timer() {
		// This constant is what stops the shell arming a timer for a count-based
		// window. A count window has no instant to arm against - its coordinate is an
		// arrival ordinal - so it seals on arrival and answers none from the ledger.
		assert!(EventTime::SEALS_ON_TIMER);
		assert!(!Ordinal::SEALS_ON_TIMER);
	}
}
