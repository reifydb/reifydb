// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{interface::catalog::queue::Queue, partition, partition::QueuePlacement};
#[cfg(test)]
use reifydb_value::value::partition::Partition;

use crate::Result;

pub fn ordered_by_index(queue: &Queue) -> Result<Option<usize>> {
	partition::ordered_by_index(queue)
}

pub fn partition_by_indices(queue: &Queue) -> Vec<usize> {
	partition::partition_by_indices(queue)
}

pub fn placement_of(
	shape: &RowShape,
	row: &EncodedQueueRow,
	partition_by: &[usize],
	ordered_by: Option<usize>,
) -> QueuePlacement {
	partition::placement_of(shape, row, partition_by, ordered_by)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_a_queue_without_partition_columns_is_one_fifo_at_the_named_partition() {
		// Any other partition here would spread an unpartitioned queue away from where claim and drop look.
		let placement = QueuePlacement::new(None, Some(Partition(1)));

		assert_eq!(placement.partition, Queue::UNPARTITIONED);
		assert_eq!(Queue::UNPARTITIONED, Partition(0));
	}

	#[test]
	fn test_the_partition_is_the_full_hash_of_the_partition_values() {
		// A truncated hash would merge two partitions into one FIFO and put them under one lock.
		let hash = Partition(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210);

		assert_eq!(QueuePlacement::new(Some(hash), None).partition, hash);
	}

	#[test]
	fn test_a_keyed_queue_truncates_the_hash_and_an_unkeyed_one_reports_no_key() {
		// The key hash is the one-active-per-key discriminator, so an unkeyed row must not block on anything.
		let hash = Partition(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210);

		assert_eq!(QueuePlacement::new(None, Some(hash)).key_hash, Some(0xFEDC_BA98_7654_3210));
		assert_eq!(QueuePlacement::new(Some(hash), None).key_hash, None);
	}
}
