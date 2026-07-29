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
	pub activity_bucket: u64,
}

impl GroupRecord {
	pub const RECLAIMED_BUCKET: u64 = u64::MAX;

	pub fn new(group: impl Into<Vec<u8>>, activity_bucket: u64) -> Self {
		Self {
			group: group.into(),
			activity_bucket,
		}
	}

	pub fn reclaimed(group: impl Into<Vec<u8>>) -> Self {
		Self::new(group, Self::RECLAIMED_BUCKET)
	}

	pub fn is_data_reclaimed(&self) -> bool {
		self.activity_bucket == Self::RECLAIMED_BUCKET
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

	use super::{ActivityBuckets, GroupRecord};
	use crate::state::horizon::{Cutoff, Position};

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("test duration must be representable")
	}

	#[test]
	fn the_reclaimed_marker_is_out_of_reach_of_every_live_position() {
		// Phase 1 parks the record at a bucket no activity can produce, and that is what forces the
		// group's next event to re-stamp instead of reusing the bucket it already cached. If a real
		// position could reach the marker, a live group would read as data-reclaimed and phase 2 would
		// take the row-number mapping out from under a sink row that still names it.
		for width in [1u64, 7, 100, 4096] {
			let buckets = ActivityBuckets::undeclared(width);
			for position in [0u64, 1, 1_000, i64::MAX as u64] {
				assert_ne!(
					buckets.of(Position(DateTime::from_nanos(position))),
					GroupRecord::RECLAIMED_BUCKET,
					"width {width}: position {position} reaches the reclaimed marker"
				);
			}
		}
	}

	#[test]
	fn a_reclaimed_record_still_names_the_group_it_came_from() {
		// Phase 2 resolves the id back to its bytes to clear the dictionary entry, and it runs long
		// after phase 1 marked the record. Dropping the bytes at marking time would strand that entry:
		// one leaked row per reclaimed group, in the very table the scan walks.
		let marked = GroupRecord::reclaimed(b"a-group".to_vec());

		assert!(marked.is_data_reclaimed());
		assert_eq!(marked.group, b"a-group".to_vec());
		assert!(!GroupRecord::new(b"a-group".to_vec(), 7).is_data_reclaimed());
	}

	#[test]
	fn a_bucket_is_only_due_once_the_cutoff_has_passed_its_whole_span() {
		// The index records which bucket a group was last active in, not the exact position. A group
		// stamped at the very end of bucket b was active until (b + 1) * width - 1, so a cutoff that
		// merely reaches into bucket b must NOT make it due - that would reclaim state the operator
		// is still using. Coarse buckets may only ever delay reclamation, never advance it.
		let buckets = ActivityBuckets::undeclared(100);

		assert_eq!(buckets.of(Position(DateTime::from_nanos(0))), 0);
		assert_eq!(buckets.of(Position(DateTime::from_nanos(99))), 0);
		assert_eq!(buckets.of(Position(DateTime::from_nanos(100))), 1);

		assert_eq!(buckets.first_live(Cutoff(DateTime::from_nanos(0))), 0, "nothing is due before any time has passed");
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
		// Width is a churn/latency knob: wider buckets mean fewer index rewrites and later
		// reclamation. What must hold for every width is that a group stamped at `position` is never
		// reported due while the cutoff is at or below that position, or activity would be discarded
		// as idleness.
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
		// Width arrives from configuration, so zero must degrade to exact per-position stamping
		// instead of panicking the flow actor on its first batch. Both the raw grid and the event
		// grid take their width from a declaration, so both have to survive a zero.
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
		// The undeclared grid is what a perpetual node buckets in: it has no unit, so it divides the
		// raw coordinate directly. Its arithmetic must stay byte-for-byte what it was, or a node that
		// declares no horizon would start rewriting its activity index on a different schedule.
		// This is what survives of the version-domain quantisation test: the version domain is gone,
		// but the unit-less division it shared with the undeclared grid is still load-bearing.
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
		// THE test for this stage's unit change. Event buckets moved from millisecond integers to
		// nanosecond instants, which scales BOTH the width and the position by 1_000_000. Division
		// is invariant under that, so every bucket index a millis-era grid produced must still be
		// produced - the change buys resolution below a millisecond without moving any boundary that
		// existed before. A grid or a position scaled by a different factor than the other would
		// silently move every group's bucket, and only comparing against the old arithmetic sees it.
		// Mutation: build the grid from millis but the position from nanos (or vice versa) and the
		// quotients diverge by a factor of a million.
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
		// The point of carrying nanoseconds rather than milliseconds: a sub-millisecond span used to
		// truncate to a zero width and collapse every group into one bucket. It now quantises for
		// real, which is what lets a short seal horizon reclaim on its own schedule.
		let grid = ActivityBuckets::event(Duration::from_nanoseconds(250).unwrap())
			.event_grid()
			.expect("event grid");

		assert_eq!(grid.of(DateTime::from_nanos(249)), 0);
		assert_eq!(grid.of(DateTime::from_nanos(250)), 1);
		assert_eq!(grid.first_live(DateTime::from_nanos(500)), 2);
	}
}
