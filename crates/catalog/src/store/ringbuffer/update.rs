// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::row::EncodedRow,
	interface::catalog::{
		id::RingBufferId,
		ringbuffer::{RingBuffer, RingBufferMetadata},
	},
	key::ringbuffer::RingBufferMetadataKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction};
use reifydb_type::value::Value;

use crate::{CatalogStore, Result, store::ringbuffer::shape::ringbuffer_metadata};

pub fn encode_ringbuffer_metadata(metadata: &RingBufferMetadata) -> EncodedRow {
	let mut row = ringbuffer_metadata::SHAPE.allocate();
	ringbuffer_metadata::SHAPE.set_u64(&mut row, ringbuffer_metadata::ID, metadata.id);
	ringbuffer_metadata::SHAPE.set_u64(&mut row, ringbuffer_metadata::CAPACITY, metadata.capacity);
	ringbuffer_metadata::SHAPE.set_u64(&mut row, ringbuffer_metadata::HEAD, metadata.head);
	ringbuffer_metadata::SHAPE.set_u64(&mut row, ringbuffer_metadata::TAIL, metadata.tail);
	ringbuffer_metadata::SHAPE.set_u64(&mut row, ringbuffer_metadata::COUNT, metadata.count);
	row
}

pub fn decode_ringbuffer_metadata(row: &EncodedRow) -> RingBufferMetadata {
	RingBufferMetadata {
		id: RingBufferId(ringbuffer_metadata::SHAPE.get_u64(row, ringbuffer_metadata::ID)),
		capacity: ringbuffer_metadata::SHAPE.get_u64(row, ringbuffer_metadata::CAPACITY),
		count: ringbuffer_metadata::SHAPE.get_u64(row, ringbuffer_metadata::COUNT),
		head: ringbuffer_metadata::SHAPE.get_u64(row, ringbuffer_metadata::HEAD),
		tail: ringbuffer_metadata::SHAPE.get_u64(row, ringbuffer_metadata::TAIL),
	}
}

impl CatalogStore {
	pub(crate) fn update_ringbuffer_metadata(
		txn: &mut CommandTransaction,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(&metadata);
		txn.set(&RingBufferMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}

	pub(crate) fn update_ringbuffer_metadata_admin(
		txn: &mut AdminTransaction,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(&metadata);
		txn.set(&RingBufferMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}

	pub(crate) fn update_ringbuffer_metadata_txn(
		txn: &mut Transaction<'_>,
		metadata: RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(&metadata);
		txn.set(&RingBufferMetadataKey::encoded(metadata.id), row)?;
		Ok(())
	}

	pub(crate) fn update_ringbuffer_partition_metadata(
		txn: &mut CommandTransaction,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(metadata);
		let key = RingBufferMetadataKey::encoded_partition(ringbuffer, partition_values.to_vec());
		txn.set(&key, row)?;
		Ok(())
	}

	pub(crate) fn save_partition_metadata(
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition_key: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		if ringbuffer.partition_by.is_empty() {
			Self::update_ringbuffer_metadata_txn(txn, metadata.clone())
		} else {
			Self::update_ringbuffer_partition_metadata_txn(txn, ringbuffer.id, partition_key, metadata)
		}
	}

	pub(crate) fn update_ringbuffer_partition_metadata_txn(
		txn: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
		partition_values: &[Value],
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(metadata);
		let key = RingBufferMetadataKey::encoded_partition(ringbuffer, partition_values.to_vec());
		txn.set(&key, row)?;
		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::test_utils::ensure_test_ringbuffer;

	#[test]
	fn test_update_ringbuffer_metadata() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		// Get initial metadata
		let mut metadata =
			CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
				.unwrap()
				.expect("Metadata should exist");

		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 1);
		assert_eq!(metadata.tail, 1);

		// Update metadata
		metadata.count = 5;
		metadata.head = 2;
		metadata.tail = 7;

		CatalogStore::update_ringbuffer_metadata_admin(&mut txn, metadata.clone()).unwrap();

		// Verify update
		let updated = CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(updated.count, 5);
		assert_eq!(updated.head, 2);
		assert_eq!(updated.tail, 7);
		assert_eq!(updated.capacity, metadata.capacity);
	}

	#[test]
	fn test_update_ringbuffer_metadata_wrap_around() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let mut metadata =
			CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
				.unwrap()
				.expect("Metadata should exist");

		// Simulate wrap-around scenario
		metadata.count = metadata.capacity;
		metadata.head = metadata.capacity - 1;
		metadata.tail = 0;

		CatalogStore::update_ringbuffer_metadata_admin(&mut txn, metadata.clone()).unwrap();

		let updated = CatalogStore::find_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(updated.count, metadata.capacity);
		assert_eq!(updated.head, metadata.capacity - 1);
		assert_eq!(updated.tail, 0);
	}
}
