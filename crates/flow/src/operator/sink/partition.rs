// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::object::ObjectId, partition::PartitionError, value::column::columns::Columns};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition},
};

pub fn partition_of(indices: &[usize], columns: &Columns, row_idx: usize) -> (Partition, Vec<Value>) {
	let values: Vec<Value> = indices.iter().map(|&i| columns.data_at(i).get_value(row_idx)).collect();
	(Partition::of(&values), values)
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
