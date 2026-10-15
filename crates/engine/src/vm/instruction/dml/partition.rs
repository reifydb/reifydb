// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::ringbuffer::{RingBuffer, RingBufferMetadata},
	key::{
		any::TaggedKey,
		row::{PartitionedRowKey, RowKey, StorageRowKey},
	},
	partition::{BatchPartitions, partition_col_indices, partitions_of_batch},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::{
	reifydb_assertions,
	value::{Value, partition::Partition, row_number::RowNumber},
};

use super::columns::CastColumns;
use crate::Result;

#[inline]
pub(super) fn ensure_partition_metadata(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	partition_key: &[Value],
	cache: &mut HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	if !cache.contains_key(partition_key) {
		let existing = catalog.find_partition_metadata(txn, ringbuffer, partition_key)?;
		let m = existing.unwrap_or_else(RingBufferMetadata::new);
		cache.insert(partition_key.to_vec(), m);
	}
	Ok(())
}

#[derive(Default)]
pub(super) struct VictimProbe {
	stored: HashMap<Option<Partition>, Vec<RowNumber>>,
	read_to: u64,
}

impl VictimProbe {
	fn partition_rows(
		&mut self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition: Partition,
		want: usize,
	) -> Result<&[RowNumber]> {
		if !self.stored.contains_key(&Some(partition)) {
			let range = PartitionedRowKey::partition_scan_range(ringbuffer.id, partition, None);
			let mut rows = Vec::with_capacity(want);
			for entry in txn.range_rev(range, RangeScope::All, want)?.take(want) {
				if let TaggedKey::PartitionedRow(pk) = &entry?.key {
					rows.push(pk.row);
				}
			}
			self.stored.insert(Some(partition), rows);
		}
		Ok(&self.stored[&Some(partition)])
	}

	fn load_rows(
		&mut self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		metadata: &RingBufferMetadata,
		want: usize,
	) -> Result<()> {
		if self.stored.contains_key(&None) {
			return Ok(());
		}
		let read_to = metadata.tail.min(metadata.head.saturating_add(want as u64));
		let mut rows = if metadata.head < read_to {
			txn.range_row(
				ringbuffer.id.into(),
				Bound::Included(StorageRowKey::new(RowNumber(read_to - 1))),
				Bound::Included(StorageRowKey::new(RowNumber(metadata.head))),
				RangeScope::All,
				want,
			)?
			.map(|entry| entry.map(|entry| entry.key.row()))
			.collect::<Result<Vec<_>>>()?
		} else {
			Vec::new()
		};
		rows.sort();
		self.read_to = read_to;
		self.stored.insert(None, rows);
		Ok(())
	}

	fn stored(
		&mut self,
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		row_number: RowNumber,
	) -> Result<bool> {
		if row_number.0 < self.read_to {
			return Ok(self.stored[&None].binary_search(&row_number).is_ok());
		}
		Ok(txn.get(&RowKey::new(ringbuffer.id, row_number))?.is_some())
	}
}

#[allow(clippy::too_many_arguments)]
pub(super) fn select_oldest_for_partition(
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	partition: Option<Partition>,
	metadata: &mut RingBufferMetadata,
	chosen: &HashSet<RowNumber>,
	pending: &HashSet<RowNumber>,
	probe: &mut VictimProbe,
	remaining: usize,
) -> Result<Option<RowNumber>> {
	if let Some(partition) = partition {
		let rows = probe.partition_rows(txn, ringbuffer, partition, remaining)?;
		let mut oldest = rows.iter().copied().find(|row| !chosen.contains(row));
		if oldest.is_none() && rows.len() == remaining {
			let range = PartitionedRowKey::partition_scan_range(ringbuffer.id, partition, None);
			for entry in txn.range_rev(range, RangeScope::All, chosen.len() + 1)? {
				if let TaggedKey::PartitionedRow(pk) = &entry?.key
					&& !chosen.contains(&pk.row)
				{
					oldest = Some(pk.row);
					break;
				}
			}
		}
		metadata.count -= 1;
		return Ok(oldest);
	}

	probe.load_rows(txn, ringbuffer, metadata, remaining + 1)?;
	let mut live = |txn: &mut Transaction<'_>, position: u64| -> Result<bool> {
		let row_number = RowNumber(position);
		if pending.contains(&row_number) {
			return Ok(true);
		}
		Ok(!chosen.contains(&row_number) && probe.stored(txn, ringbuffer, row_number)?)
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
pub(crate) fn save_all_partition_metadata(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ringbuffer: &RingBuffer,
	cache: &HashMap<Vec<Value>, RingBufferMetadata>,
) -> Result<()> {
	for (partition_key, m) in cache {
		if m.is_empty() {
			catalog.remove_partition_metadata(txn, ringbuffer, partition_key)?;
		} else {
			catalog.save_partition_metadata(txn, ringbuffer, partition_key, m)?;
		}
	}
	Ok(())
}

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
