// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::{deserializer::KeyDeserializer, encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			id::RingBufferId,
			ringbuffer::{RingBufferMetadata, decode_ringbuffer_metadata},
			shape::ShapeId,
		},
		store::{EntryKind, MultiVersionContains},
	},
	key::{
		partitioned_row::{PartitionedRowKey, RowLocator},
		ringbuffer::RingBufferMetadataKey,
		row::RowKey,
	},
};
use reifydb_value::{
	Result,
	value::{Value, partition::Partition, row_number::RowNumber},
};

use crate::{
	MultiVersionScope,
	store::{StandardMultiStore, multi::MultiVersionRangeCursor},
	tier::TierStorage,
};

pub const CURRENT: CommitVersion = CommitVersion(u64::MAX);

pub struct PartitionEntry {
	pub metadata: RingBufferMetadata,
	pub partition_values: Vec<Value>,
}

fn decode_partition_values(key: &[u8]) -> Vec<Value> {
	let mut de = KeyDeserializer::from_bytes(key);
	let _ = (de.read_u8(), de.read_u64());
	let mut partition_values = vec![];
	while !de.is_empty() {
		match de.read_value() {
			Ok(value) => partition_values.push(value),
			Err(_) => break,
		}
	}
	partition_values
}

pub fn scan_partition_metadata_batch(
	store: &StandardMultiStore,
	ringbuffer: RingBufferId,
	cursor: &mut MultiVersionRangeCursor,
	batch_size: u64,
) -> Result<(Vec<PartitionEntry>, bool)> {
	let range = RingBufferMetadataKey::full_scan_for_ringbuffer(ringbuffer);
	let batch = store.range_next(
		cursor,
		range,
		MultiVersionScope::AsOf {
			read: CURRENT,
		},
		batch_size,
	)?;

	let entries = batch
		.items
		.into_iter()
		.map(|row| PartitionEntry {
			metadata: decode_ringbuffer_metadata(&row.row),
			partition_values: decode_partition_values(row.key.as_slice()),
		})
		.collect();

	Ok((entries, batch.has_more))
}

pub fn head_row_key(ringbuffer: RingBufferId, partition_values: &[Value], head: u64) -> EncodedKey {
	if partition_values.is_empty() {
		RowKey::encoded(ringbuffer, RowNumber(head))
	} else {
		let partition = Partition::of(partition_values);
		PartitionedRowKey::encoded(ShapeId::ringbuffer(ringbuffer), partition, RowLocator::Row(RowNumber(head)))
	}
}

pub fn head_row_exists(store: &StandardMultiStore, key: &EncodedKey) -> Result<bool> {
	store.contains(key, CURRENT)
}

pub fn has_any_live_row(
	store: &StandardMultiStore,
	ringbuffer: RingBufferId,
	partition_values: &[Value],
) -> Result<bool> {
	let range = if partition_values.is_empty() {
		RowKey::full_scan(ringbuffer)
	} else {
		let partition = Partition::of(partition_values);
		PartitionedRowKey::partition_range(ShapeId::ringbuffer(ringbuffer), partition)
	};

	let mut cursor = MultiVersionRangeCursor::new();
	let batch = store.range_next(
		&mut cursor,
		range,
		MultiVersionScope::AsOf {
			read: CURRENT,
		},
		1,
	)?;
	Ok(!batch.items.is_empty())
}

pub fn remove_partition_metadata_key(store: &StandardMultiStore, key: &EncodedKey) -> Result<()> {
	store.invalidate_read_key(key);

	if let Some(buffer) = store.commit() {
		drop_all_versions(buffer, key)?;
	}
	if let Some(persistent) = store.persistent() {
		drop_all_versions(persistent, key)?;
	}
	Ok(())
}

fn drop_all_versions<S: TierStorage>(storage: &S, key: &EncodedKey) -> Result<()> {
	let versions = storage.get_all_versions(EntryKind::Multi, key.as_slice())?;
	if versions.is_empty() {
		return Ok(());
	}

	let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
	batches.insert(EntryKind::Multi, versions.into_iter().map(|(version, _)| (key.clone(), version)).collect());
	storage.drop(batches)
}
