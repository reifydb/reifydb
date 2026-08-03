// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const DISK_PAYLOAD_BYTES: &str = "disk_payload_bytes";

pub const STATE_RESIDENT_BYTES: &str = "state_resident_bytes";
pub const STATE_DIRTY_BYTES: &str = "state_dirty_bytes";
pub const STATE_MEMBERSHIP_BYTES: &str = "state_membership_bytes";
pub const STATE_POOL_BUDGET: &str = "state_pool_budget";

pub const ROW_NUMBER_CACHE_BYTES: &str = "row_number_cache_bytes";
pub const ROW_NUMBER_MEMBERSHIP_BYTES: &str = "row_number_membership_bytes";

pub const GROUP_CACHE_BYTES: &str = "group_cache_bytes";
pub const GROUP_MEMBERSHIP_BYTES: &str = "group_membership_bytes";
pub const GROUP_DUE_BYTES: &str = "group_due_bytes";

pub const MEMORY_BYTES: &[&str] = &[
	STATE_RESIDENT_BYTES,
	STATE_DIRTY_BYTES,
	STATE_MEMBERSHIP_BYTES,
	ROW_NUMBER_CACHE_BYTES,
	ROW_NUMBER_MEMBERSHIP_BYTES,
	GROUP_CACHE_BYTES,
	GROUP_MEMBERSHIP_BYTES,
	GROUP_DUE_BYTES,
];

#[cfg(test)]
mod tests {
	use super::{
		DISK_PAYLOAD_BYTES, GROUP_CACHE_BYTES, GROUP_DUE_BYTES, GROUP_MEMBERSHIP_BYTES, MEMORY_BYTES,
		STATE_POOL_BUDGET,
	};

	#[test]
	fn the_memory_set_carries_every_group_heap_metric() {
		// A heap metric that is emitted but missing from MEMORY_BYTES reads as zero to anyone
		// summing an operator's memory, so the operator under-reports silently instead of failing.
		for metric in [GROUP_CACHE_BYTES, GROUP_MEMBERSHIP_BYTES, GROUP_DUE_BYTES] {
			assert!(
				MEMORY_BYTES.contains(&metric),
				"{metric} is emitted as heap but excluded from MEMORY_BYTES"
			);
		}
	}

	#[test]
	fn the_memory_set_excludes_disk_and_the_pool_budget() {
		// MEMORY_BYTES is what a consumer sums to answer "how much memory does this operator
		// hold". Disk is not memory, and the pool budget is the operator's LIMIT rather than its
		// consumption, so including either would report an operator as holding bytes it does not.
		assert!(!MEMORY_BYTES.contains(&DISK_PAYLOAD_BYTES), "disk payload is not resident memory");
		assert!(!MEMORY_BYTES.contains(&STATE_POOL_BUDGET), "the pool budget is a limit, not usage");
	}

	#[test]
	fn every_memory_metric_is_named_once() {
		// A duplicate would be summed twice by every consumer of the set.
		let mut seen = MEMORY_BYTES.to_vec();
		seen.sort_unstable();
		let before = seen.len();
		seen.dedup();
		assert_eq!(seen.len(), before, "MEMORY_BYTES must not repeat a metric name");
	}
}
