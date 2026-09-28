// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{object::ObjectId, view::View},
	partition::{PartitionError, partition_of as core_partition_of},
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition},
};

pub fn partition_of(view: &View, indices: &[usize], columns: &Columns, row_idx: usize) -> (Partition, Vec<Value>) {
	let values: Vec<Value> = indices.iter().map(|&i| columns.data_at(i).get_value(row_idx)).collect();
	(core_partition_of(view.columns(), view.partition_by(), &values), values)
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
