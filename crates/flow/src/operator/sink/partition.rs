// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	interface::catalog::{object::ObjectId, view::View},
	partition::{PartitionError, partition_of as core_partition_of},
};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition},
};

use crate::operator::sink::value_at;

pub fn partition_of(
	view: &View,
	indices: &[usize],
	columns: &RecordBatch,
	row_idx: usize,
) -> Result<(Partition, Vec<Value>)> {
	let values: Vec<Value> = indices.iter().map(|&i| value_at(columns, i, row_idx)).collect::<Result<Vec<_>>>()?;
	Ok((core_partition_of(view.columns(), view.partition_by(), &values), values))
}

pub fn ensure_partition_unchanged(object: ObjectId, pre: Partition, post: Partition) -> Result<()> {
	if pre != post {
		return Err(PartitionError::ImmutablePartitionColumn {
			object,
		}
		.into());
	}
	Ok(())
}
