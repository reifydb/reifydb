// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	interface::catalog::{
		ringbuffer::{RingBuffer, RingBufferMetadata},
		shape::ShapeId,
	},
	key::{
		EncodableKey,
		partitioned_row::{PartitionedRowKey, RowLocator},
		row::RowKey,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

use super::context::RingBufferTarget;
use crate::{Result, transaction::operation::ringbuffer::RingBufferOperations, vm::services::Services};

#[inline]
pub(super) fn compute_partition_col_indices(ringbuffer: &RingBuffer) -> Vec<usize> {
	ringbuffer
		.partition_by
		.iter()
		.map(|pb_col| ringbuffer.columns.iter().position(|c| c.name == *pb_col).unwrap())
		.collect()
}

#[inline]
pub(super) fn ensure_partition_metadata(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	partition_key: &[Value],
	cache: &mut HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	if !cache.contains_key(partition_key) {
		let existing = services.catalog.find_partition_metadata(txn, target.ringbuffer, partition_key)?;
		let m = existing
			.unwrap_or_else(|| RingBufferMetadata::new(target.ringbuffer.id, target.ringbuffer.capacity));
		cache.insert(partition_key.to_vec(), m);
	}
	Ok(())
}

pub(super) fn evict_oldest_for_partition(
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	partition: Option<Partition>,
	metadata: &mut RingBufferMetadata,
) -> Result<()> {
	let ringbuffer = target.ringbuffer;

	if let Some(partition) = partition {
		let range =
			PartitionedRowKey::partition_scan_range(ShapeId::ringbuffer(ringbuffer.id), partition, None);
		let oldest = txn.range_rev(range, RangeScope::All, 1)?.next().transpose()?;
		if let Some(entry) = oldest
			&& let Some(RowLocator::Row(rn)) = PartitionedRowKey::decode(&entry.key).map(|pk| pk.locator)
		{
			txn.remove_from_ringbuffer(ringbuffer, Some(partition), rn)?;
		}
		metadata.count -= 1;
		return Ok(());
	}

	let mut evict_pos = metadata.head;
	loop {
		let key = RowKey::encoded(ringbuffer.id, RowNumber(evict_pos));
		if txn.get(&key)?.is_some() {
			txn.remove_from_ringbuffer(ringbuffer, None, RowNumber(evict_pos))?;
			break;
		}
		evict_pos += 1;
		if evict_pos >= metadata.tail {
			break;
		}
	}
	metadata.head = evict_pos + 1;
	while metadata.head < metadata.tail {
		let key = RowKey::encoded(ringbuffer.id, RowNumber(metadata.head));
		if txn.get(&key)?.is_some() {
			break;
		}
		metadata.head += 1;
	}
	metadata.count -= 1;
	Ok(())
}

#[inline]
pub(super) fn update_metadata_after_insert(metadata: &mut RingBufferMetadata, row_number: RowNumber) {
	if metadata.is_empty() {
		metadata.head = row_number.0;
	}
	metadata.count += 1;
	metadata.tail = row_number.0 + 1;
}

#[inline]
pub(super) fn save_all_partition_metadata(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	cache: &HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	for (partition_key, m) in cache {
		if m.is_empty() {
			services.catalog.remove_partition_metadata(txn, ringbuffer, partition_key)?;
		} else {
			services.catalog.save_partition_metadata(txn, ringbuffer, partition_key, m)?;
		}
	}
	Ok(())
}
