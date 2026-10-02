// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use reifydb_core::{
	interface::catalog::ringbuffer::{RingBuffer, RingBufferMetadata},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

use super::context::RingBufferTarget;
use crate::{Result, vm::services::Services};

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
		let m = existing.unwrap_or_else(RingBufferMetadata::new);
		cache.insert(partition_key.to_vec(), m);
	}
	Ok(())
}

pub(super) fn select_oldest_for_partition(
	txn: &mut Transaction<'_>,
	target: &RingBufferTarget<'_>,
	partition: Option<Partition>,
	metadata: &mut RingBufferMetadata,
	chosen: &HashSet<RowNumber>,
	pending: &HashSet<RowNumber>,
) -> Result<Option<RowNumber>> {
	let ringbuffer = target.ringbuffer;

	if let Some(partition) = partition {
		let range = PartitionedRowKey::partition_scan_range(ringbuffer.id, partition, None);
		let mut oldest = None;
		for entry in txn.range_rev(range, RangeScope::All, chosen.len() + 1)? {
			if let TaggedKey::PartitionedRow(pk) = &entry?.key
				&& !chosen.contains(&pk.row)
			{
				oldest = Some(pk.row);
				break;
			}
		}
		metadata.count -= 1;
		return Ok(oldest);
	}

	let live = |txn: &mut Transaction<'_>, position: u64| -> Result<bool> {
		let row_number = RowNumber(position);
		if pending.contains(&row_number) {
			return Ok(true);
		}
		Ok(!chosen.contains(&row_number) && txn.get(&RowKey::new(ringbuffer.id, row_number))?.is_some())
	};
	let mut evict_pos = metadata.head;
	let mut oldest = None;
	loop {
		if live(txn, evict_pos)? {
			oldest = Some(RowNumber(evict_pos));
			break;
		}
		evict_pos += 1;
		if evict_pos >= metadata.tail {
			break;
		}
	}
	metadata.head = evict_pos + 1;
	while metadata.head < metadata.tail {
		if live(txn, metadata.head)? {
			break;
		}
		metadata.head += 1;
	}
	metadata.count -= 1;
	Ok(oldest)
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
