// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_macro::operator_state;
use serde::{Deserialize, Serialize};

use crate::metrics::heap::HeapSize;

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
pub struct ActivityBuckets {
	width: u64,
}

impl ActivityBuckets {
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

impl HeapSize for GroupRecord {
	fn heap_size(&self) -> usize {
		self.group.capacity()
	}
}

#[cfg(test)]
mod tests {
	use super::{ActivityBuckets, GroupRecord};

	#[test]
	fn the_reclaimed_marker_is_out_of_reach_of_every_live_position() {
		// Phase 1 parks the record at a bucket no activity can produce, and that is what forces the
		// group's next event to re-stamp instead of reusing the bucket it already cached. If a real
		// position could reach the marker, a live group would read as data-reclaimed and phase 2 would
		// take the row-number mapping out from under a sink row that still names it.
		for width in [1u64, 7, 100, 4096] {
			let buckets = ActivityBuckets::new(width);
			for position in [0u64, 1, 1_000, i64::MAX as u64] {
				assert_ne!(
					buckets.of(position),
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
		let buckets = ActivityBuckets::new(100);

		assert_eq!(buckets.of(0), 0);
		assert_eq!(buckets.of(99), 0);
		assert_eq!(buckets.of(100), 1);

		assert_eq!(buckets.first_live(0), 0, "nothing is due before any time has passed");
		assert_eq!(buckets.first_live(99), 0, "a cutoff inside bucket 0 must not retire bucket 0");
		assert_eq!(buckets.first_live(100), 1, "bucket 0 retires only once the cutoff clears its end");
		assert_eq!(buckets.first_live(250), 2, "bucket 2 is still live while the cutoff sits inside it");
	}

	#[test]
	fn bucket_width_changes_timing_but_never_correctness() {
		// Width is a churn/latency knob: wider buckets mean fewer index rewrites and later
		// reclamation. What must hold for every width is that a group stamped at `position` is never
		// reported due while the cutoff is at or below that position, or activity would be discarded
		// as idleness.
		for width in [1u64, 7, 100, 4096] {
			let buckets = ActivityBuckets::new(width);
			for position in [0u64, 1, 63, 99, 100, 5000] {
				let bucket = buckets.of(position);
				assert!(
					bucket >= buckets.first_live(position),
					"width {width}: a group stamped at {position} was reported due at a cutoff \
					 of {position}"
				);
			}
		}
	}

	#[test]
	fn a_zero_width_is_clamped_rather_than_dividing_by_zero() {
		// Width arrives from configuration, so zero must degrade to exact per-position stamping
		// instead of panicking the flow actor on its first batch.
		let buckets = ActivityBuckets::new(0);

		assert_eq!(buckets.width(), 1);
		assert_eq!(buckets.of(42), 42);
	}
}
