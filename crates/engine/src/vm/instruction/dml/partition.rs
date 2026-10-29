// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::ringbuffer::RingBuffer,
	partition::{BatchPartitions, partition_col_indices, partitions_of_batch},
};
use reifydb_value::reifydb_assertions;

use super::columns::CastColumns;
use crate::Result;

pub(crate) fn ringbuffer_batch_partitions(ringbuffer: &RingBuffer, cast: &CastColumns) -> Result<BatchPartitions> {
	let indices = partition_col_indices(&ringbuffer.columns, &ringbuffer.partition_by);
	if indices.is_empty() {
		return Ok(BatchPartitions::default());
	}
	let keys = indices.iter().map(|&index| cast.view(index)).collect::<Result<Vec<_>>>()?;
	let batch = partitions_of_batch(&keys);
	reifydb_assertions! {
		for (partition, values) in &batch.distinct {
			assert_eq!(
				reifydb_core::partition::partition_of(&ringbuffer.columns, &ringbuffer.partition_by, values),
				*partition,
				"a batch partition must hash its values like the row-at-a-time partition"
			);
		}
	}
	Ok(batch)
}
