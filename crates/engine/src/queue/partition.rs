// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{queue::EncodedQueueRow, shape::RowShape};
use reifydb_core::{interface::catalog::queue::Queue, internal_error};
use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

use crate::Result;

pub struct QueuePlacement {
	pub partition: u16,
	pub key_hash: Option<u64>,
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

pub fn placement_of(
	queue: &Queue,
	shape: &RowShape,
	row: &EncodedQueueRow,
	ordered_by_index: Option<usize>,
	row_number: RowNumber,
) -> QueuePlacement {
	let hash = match ordered_by_index {
		Some(index) => Partition::of(&[shape.get_value(row.as_slice(), index)]),
		None => Partition::of(&[Value::Uint8(row_number.0)]),
	};
	placement_from_hash(hash, queue.partitions(), ordered_by_index.is_some())
}

fn placement_from_hash(hash: Partition, partitions: u16, keyed: bool) -> QueuePlacement {
	QueuePlacement {
		partition: (hash.0 % partitions as u128) as u16,
		key_hash: keyed.then_some(hash.0 as u64),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_partition_is_the_full_hash_modulo_the_partition_count() {
		let hash = Partition(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210);

		let placement = placement_from_hash(hash, 1000, true);

		assert_eq!(placement.partition, 40);
		assert_eq!(((hash.0 as u64) % 1000) as u16, 720, "the truncated hash would have placed it elsewhere");
	}

	#[test]
	fn test_a_keyed_queue_truncates_the_hash_and_an_unkeyed_one_reports_no_key() {
		let hash = Partition(0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210);

		assert_eq!(placement_from_hash(hash, 16, true).key_hash, Some(0xFEDC_BA98_7654_3210));
		assert_eq!(placement_from_hash(hash, 16, false).key_hash, None);
	}

	#[test]
	fn test_a_single_partition_queue_places_everything_in_partition_zero() {
		for hash in [0u128, 1, u128::MAX] {
			assert_eq!(placement_from_hash(Partition(hash), 1, true).partition, 0);
		}
	}
}
