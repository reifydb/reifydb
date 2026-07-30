// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::{coord::OrdinalCoord, kind::ordinal_window_span, policy::SealPolicy, span::WindowSpan};

pub struct TumblingOverRows {
	capacity: u64,
}

impl TumblingOverRows {
	pub fn holding(capacity: u64) -> Self {
		Self {
			capacity: capacity.max(1),
		}
	}

	pub fn capacity(&self) -> u64 {
		self.capacity
	}

	pub fn window_id(&self, coord: OrdinalCoord) -> u64 {
		coord.value() / self.capacity
	}

	pub fn span(&self, coord: OrdinalCoord) -> WindowSpan<DateTime> {
		ordinal_window_span(self.window_id(coord))
	}
}

pub struct TumblingOverTime {
	size: Duration,
}

impl TumblingOverTime {
	pub fn new(size: Duration) -> Self {
		Self {
			size,
		}
	}

	pub fn seal_policy(&self, grace: Duration) -> SealPolicy {
		SealPolicy::tumbling(self.size, grace)
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
		// `window tumbling { .. } with { count: 0 }` compiles - build_measure accepts any
		// value - and every ordinal is then divided by the capacity. The host clamps this at each
		// of its three call sites with `.unwrap_or(1).max(1)`; folding the clamp into the
		// constructor means a fourth call site cannot forget it.
		// count: 0 silently becomes a window of one row rather than being refused. Unlike a
		// zero sliding slide there is no crash and no unbounded window, so refusing it would
		// be a separate behaviour change.
		assert_eq!(TumblingOverRows::holding(0).capacity(), 1);
		assert_eq!(TumblingOverRows::holding(0).window_id(ordinal(7)), 7);
	}

	#[test]
	fn every_capacity_rows_advance_the_window_by_exactly_one() {
		// Tumbling windows are disjoint and adjacent - row N and row N+capacity must land
		// in consecutive windows with nothing between them. An off-by-one in either direction
		// either overlaps two windows (double counting) or leaves a gap (rows in no window).
		let rows = TumblingOverRows::holding(4);

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
		let rows = TumblingOverRows::holding(4);

		assert_ne!(rows.span(ordinal(3)), rows.span(ordinal(4)));
		assert_eq!(rows.span(ordinal(0)), rows.span(ordinal(3)));
	}
}
