// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{bytes::EncodedBytes, shape::RowShape};
use reifydb_core::{interface::catalog::queue::Queue, internal_error};
use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

use crate::Result;

pub fn ordered_by_index(queue: &Queue) -> Result<Option<usize>> {
	let Some(ordered_by) = queue.ordered_by() else {
		return Ok(None);
	};
	let index = queue.columns.iter().position(|c| c.name == *ordered_by).ok_or_else(|| {
		internal_error!("queue {} declares ordered_by {} which is not a column", queue.name, ordered_by)
	})?;
	Ok(Some(index))
}

pub fn partition_of(
	queue: &Queue,
	shape: &RowShape,
	row: &EncodedBytes,
	ordered_by_index: Option<usize>,
	row_number: RowNumber,
) -> u16 {
	let hash = match ordered_by_index {
		Some(index) => Partition::of(&[shape.get_value(row, index)]),
		None => Partition::of(&[Value::Uint8(row_number.0)]),
	};
	(hash.0 % queue.partitions() as u128) as u16
}
