// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::operator_state;
use reifydb_value::value::{datetime::DateTime, duration::Duration};
use serde::{Deserialize, Serialize};

use crate::{
	metrics::heap::HeapSize,
	state::horizon::{Cutoff, Position},
};

#[operator_state]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupRecord {
	pub group: Vec<u8>,
}

impl GroupRecord {
	pub fn new(group: impl Into<Vec<u8>>) -> Self {
		Self {
			group: group.into(),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawGrid {
	width: u64,
}

impl RawGrid {
	pub fn new(width: u64) -> Self {
		Self {
			width: width.max(1),
		}
	}

	pub fn width(&self) -> u64 {
		self.width
	}

	pub fn of(&self, position: u64) -> u64 {
		position / self.width
	}

	pub fn first_live(&self, cutoff: u64) -> u64 {
		cutoff / self.width
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventGrid {
	width: Duration,
	grid: RawGrid,
}

impl EventGrid {
	pub fn new(width: Duration) -> Self {
		let nanos = width.as_nanos().ok().and_then(|nanos| u64::try_from(nanos).ok()).unwrap_or(1);
		Self {
			width,
			grid: RawGrid::new(nanos),
		}
	}

	pub fn width(&self) -> Duration {
		self.width
	}

	pub fn of(&self, position: DateTime) -> u64 {
		self.grid.of(position.to_nanos())
	}

	pub fn first_live(&self, cutoff: DateTime) -> u64 {
		self.grid.first_live(cutoff.to_nanos())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityBuckets {
	Event(EventGrid),
	Undeclared(RawGrid),
}

impl ActivityBuckets {
	pub fn event(width: Duration) -> Self {
		Self::Event(EventGrid::new(width))
	}

	pub fn undeclared(width: u64) -> Self {
		Self::Undeclared(RawGrid::new(width))
	}

	pub fn event_grid(&self) -> Option<EventGrid> {
		match self {
			Self::Event(grid) => Some(*grid),
			_ => None,
		}
	}

	pub fn of(&self, position: Position) -> u64 {
		match self {
			Self::Event(grid) => grid.of(position.instant()),
			Self::Undeclared(grid) => grid.of(position.raw()),
		}
	}

	pub fn first_live(&self, cutoff: Cutoff) -> u64 {
		match self {
			Self::Event(grid) => grid.first_live(cutoff.instant()),
			Self::Undeclared(grid) => grid.first_live(cutoff.raw()),
		}
	}
}

impl HeapSize for GroupRecord {
	fn heap_size(&self) -> usize {
		self.group.capacity()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::ActivityBuckets;
	use crate::state::horizon::{Cutoff, Position};

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	#[test]
	fn a_bucket_is_only_due_once_the_cutoff_has_passed_its_whole_span() {
		// The index records a bucket, not an exact position, so a group stamped at the end of bucket
		// b was active until (b + 1) * width - 1. A cutoff reaching into bucket b must not make it
		// due: coarse buckets may only delay reclamation, never advance it.
		let buckets = ActivityBuckets::undeclared(100);

		assert_eq!(buckets.of(Position(DateTime::from_nanos(0))), 0);
		assert_eq!(buckets.of(Position(DateTime::from_nanos(99))), 0);
		assert_eq!(buckets.of(Position(DateTime::from_nanos(100))), 1);

		assert_eq!(
			buckets.first_live(Cutoff(DateTime::from_nanos(0))),
			0,
			"nothing is due before any time has passed"
		);
		assert_eq!(
			buckets.first_live(Cutoff(DateTime::from_nanos(99))),
			0,
			"a cutoff inside bucket 0 must not retire bucket 0"
		);
		assert_eq!(
			buckets.first_live(Cutoff(DateTime::from_nanos(100))),
			1,
			"bucket 0 retires only once the cutoff clears its end"
		);
		assert_eq!(
			buckets.first_live(Cutoff(DateTime::from_nanos(250))),
			2,
			"bucket 2 is still live while the cutoff sits inside it"
		);
	}

	#[test]
	fn bucket_width_changes_timing_but_never_correctness() {
		// Width trades index rewrites for reclamation latency, but for every width a group stamped at
		// `position` must never be reported due at a cutoff at or below it, or activity reads as idle.
		for width in [1u64, 7, 100, 4096] {
			let buckets = ActivityBuckets::undeclared(width);
			for position in [0u64, 1, 63, 99, 100, 5000] {
				let bucket = buckets.of(Position(DateTime::from_nanos(position)));
				assert!(
					bucket >= buckets.first_live(Cutoff(DateTime::from_nanos(position))),
					"width {width}: a group stamped at {position} was reported due at a cutoff \
					 of {position}"
				);
			}
		}
	}

	#[test]
	fn a_zero_width_grid_is_clamped_rather_than_dividing_by_zero() {
		// Width arrives from a declaration, so zero must degrade to exact per-position stamping
		// rather than panicking the flow actor on its first batch.
		let ActivityBuckets::Undeclared(raw) = ActivityBuckets::undeclared(0) else {
			panic!("undeclared buckets carry a raw grid");
		};
		assert_eq!(raw.width(), 1);
		assert_eq!(raw.of(42), 42);

		let event = ActivityBuckets::event(Duration::zero())
			.event_grid()
			.expect("event buckets carry an event grid");
		assert_eq!(event.of(DateTime::from_nanos(42)), 42, "a zero span degrades to one bucket per nanosecond");
	}

	#[test]
	fn an_undeclared_grid_quantises_by_raw_division() {
		// The undeclared grid has no unit and divides the raw coordinate directly; changing that
		// arithmetic would make a operator declaring no horizon rewrite its activity index on a
		// different schedule.
		for width in [1u64, 7, 100, 4096] {
			for position in [0u64, 1, 99, 100, 5_000, i64::MAX as u64] {
				assert_eq!(
					ActivityBuckets::undeclared(width).of(Position(DateTime::from_nanos(position))),
					position / width
				);
			}
		}
	}

	#[test]
	fn an_event_grid_expressed_in_millis_lands_on_the_bucket_the_millis_arithmetic_did() {
		// Nanosecond instants scale both the width and the position by 1_000_000, and division is
		// invariant under that. Scaling one side by a different factor than the other would silently
		// move every group's bucket, and only comparing against the millis arithmetic catches it.
		for width_ms in [1i64, 7, 100, 4_096] {
			let grid = ActivityBuckets::event(ms(width_ms)).event_grid().expect("event grid");
			for position_ms in [0u64, 1, 99, 100, 5_000, 1_700_000_000_123] {
				assert_eq!(
					grid.of(DateTime::from_millis(position_ms)),
					position_ms / width_ms as u64,
					"width {width_ms}ms: position {position_ms}ms changed bucket"
				);
			}
		}
	}

	#[test]
	fn an_event_grid_resolves_below_the_millisecond_the_old_arithmetic_rounded_away() {
		// A sub-millisecond span truncates to a zero width in millis and collapses every group into
		// one bucket; nanoseconds are what let a short seal horizon reclaim on its own schedule.
		let grid = ActivityBuckets::event(Duration::from_nanoseconds(250).unwrap())
			.event_grid()
			.expect("event grid");

		assert_eq!(grid.of(DateTime::from_nanos(249)), 0);
		assert_eq!(grid.of(DateTime::from_nanos(250)), 1);
		assert_eq!(grid.first_live(DateTime::from_nanos(500)), 2);
	}
}
