// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const DISK_PAYLOAD_BYTES: &str = "disk_payload_bytes";

pub const STATE_RESIDENT_BYTES: &str = "state_resident_bytes";

pub const ROW_NUMBER_CACHE_BYTES: &str = "row_number_cache_bytes";
pub const ROW_NUMBER_MEMBERSHIP_BYTES: &str = "row_number_membership_bytes";

pub const MEMORY_BYTES: &[&str] = &[STATE_RESIDENT_BYTES, ROW_NUMBER_CACHE_BYTES, ROW_NUMBER_MEMBERSHIP_BYTES];

#[cfg(test)]
mod tests {
	use super::{DISK_PAYLOAD_BYTES, MEMORY_BYTES, ROW_NUMBER_CACHE_BYTES, ROW_NUMBER_MEMBERSHIP_BYTES};

	#[test]
	fn the_memory_set_carries_every_heap_metric_an_operator_emits() {
		// A heap metric missing from MEMORY_BYTES sums as zero, so the operator under-reports silently.
		for metric in [ROW_NUMBER_CACHE_BYTES, ROW_NUMBER_MEMBERSHIP_BYTES] {
			assert!(
				MEMORY_BYTES.contains(&metric),
				"{metric} is emitted as heap but excluded from MEMORY_BYTES"
			);
		}
	}

	#[test]
	fn the_memory_set_excludes_disk() {
		// MEMORY_BYTES answers how much memory an operator holds, and disk is never resident memory.
		assert!(!MEMORY_BYTES.contains(&DISK_PAYLOAD_BYTES), "disk payload is not resident memory");
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
