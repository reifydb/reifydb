// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{interface::catalog::queue::Queue, internal_error, partition::partition_col_indices};
use reifydb_value::value::{Value, partition::Partition};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuePlacement {
	pub partition: Partition,
	pub key_hash: Option<u64>,
}

impl QueuePlacement {
	pub fn new(partition: Option<Partition>, ordered: Option<Partition>) -> Self {
		Self {
			partition: partition.unwrap_or(Queue::UNPARTITIONED),
			key_hash: ordered.map(|hash| hash.0 as u64),
		}
	}
}

pub fn ordered_by_index(queue: &Queue) -> Result<Option<usize>> {
	let Some(ordered_by) = queue.ordered_by() else {
		return Ok(None);
	};
	let index = queue.columns.iter().position(|c| c.name == *ordered_by).ok_or_else(|| {
		internal_error!("queue {} declares ordered_by {} which is not a column", queue.name, ordered_by)
	})?;
	Ok(Some(index))
}

pub fn partition_by_indices(queue: &Queue) -> Vec<usize> {
	partition_col_indices(&queue.columns, &queue.partition_by)
}

pub fn placement_of(
	shape: &RowShape,
	row: &EncodedQueueRow,
	partition_by: &[usize],
	ordered_by: Option<usize>,
) -> QueuePlacement {
	QueuePlacement::new(
		stored_hash(shape, row, partition_by),
		ordered_by.and_then(|index| stored_hash(shape, row, &[index])),
	)
}

#[allow(clippy::disallowed_methods)]
fn stored_hash(shape: &RowShape, row: &EncodedQueueRow, indices: &[usize]) -> Option<Partition> {
	if indices.is_empty() {
		return None;
	}
	let values: Vec<Value> = indices.iter().map(|&index| shape.get_value(row.as_slice(), index)).collect();
	Some(Partition::of(&values))
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
