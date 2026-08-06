// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::window::{
	coord::{OrdinalCoord, RowSpan},
	kind::ordinal_window_span,
	span::WindowSpan,
};

pub struct TumblingOverRows {
	capacity: RowSpan,
}

impl TumblingOverRows {
	pub fn holding(capacity: RowSpan) -> Self {
		Self {
			capacity: RowSpan::of(capacity.rows().max(1)),
		}
	}

	pub fn capacity(&self) -> RowSpan {
		self.capacity
	}

	pub fn window_id(&self, coord: OrdinalCoord) -> u64 {
		coord.value() / self.capacity.rows()
	}

	pub fn span(&self, coord: OrdinalCoord) -> WindowSpan<DateTime> {
		ordinal_window_span(self.window_id(coord))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ordinal(value: u64) -> OrdinalCoord {
		OrdinalCoord::from_arrival_counter(value)
	}

	#[test]
	fn a_capacity_of_zero_collapses_to_one_rather_than_dividing_by_zero() {
		// `with { count: 0 }` compiles and every ordinal is then divided by the capacity. Clamping in
		// the constructor rather than at each call site means a new caller cannot forget it; a zero
		// count degrades to a one-row window rather than being refused.
		assert_eq!(TumblingOverRows::holding(RowSpan::of(0)).capacity(), RowSpan::of(1));
		assert_eq!(TumblingOverRows::holding(RowSpan::of(0)).window_id(ordinal(7)), 7);
	}

	#[test]
	fn every_capacity_rows_advance_the_window_by_exactly_one() {
		// Tumbling windows are disjoint and adjacent, so an off-by-one either overlaps two windows
		// (double counting) or leaves a gap (rows in no window).
		let rows = TumblingOverRows::holding(RowSpan::of(4));

		assert_eq!(rows.window_id(ordinal(0)), 0);
		assert_eq!(rows.window_id(ordinal(3)), 0);
		assert_eq!(rows.window_id(ordinal(4)), 1);
		assert_eq!(rows.window_id(ordinal(7)), 1);
		assert_eq!(rows.window_id(ordinal(8)), 2);
	}

	#[test]
	fn consecutive_windows_never_share_a_span() {
		// The engine keys by span, so two window ids resolving to one span would merge two
		// windows into a single accumulator and publish their sum as one row.
		let rows = TumblingOverRows::holding(RowSpan::of(4));

		assert_ne!(rows.span(ordinal(3)), rows.span(ordinal(4)));
		assert_eq!(rows.span(ordinal(0)), rows.span(ordinal(3)));
	}
}
