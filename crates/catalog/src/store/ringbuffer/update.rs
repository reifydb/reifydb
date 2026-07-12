// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::RingBufferId,
		ringbuffer::{RingBuffer, RingBufferMetadata, encode_ringbuffer_metadata},
	},
	key::ringbuffer::RingBufferMetadataKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction, command::CommandTransaction};
use reifydb_value::value::Value;

use crate::{CatalogStore, Result};

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

	pub(crate) fn remove_partition_metadata(
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition_key: &[Value],
	) -> Result<()> {
		if ringbuffer.partition_by.is_empty() {
			txn.remove(&RingBufferMetadataKey::encoded(ringbuffer.id))
		} else {
			txn.remove(&RingBufferMetadataKey::encoded_partition(ringbuffer.id, partition_key.to_vec()))
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
