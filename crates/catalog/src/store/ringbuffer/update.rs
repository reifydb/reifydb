// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::ringbuffer::{RingBuffer, RingBufferMetadata, encode_ringbuffer_metadata},
	key::ringbuffer::RingBufferMetadataKey,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::partition::Partition;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn save_partition_metadata(
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
		metadata: &RingBufferMetadata,
	) -> Result<()> {
		let row = encode_ringbuffer_metadata(metadata);
		txn.set(vec![(RingBufferMetadataKey::of(ringbuffer.id, partition).into(), row.into_bytes())])?;
		Ok(())
	}

	pub(crate) fn remove_partition_metadata(
		txn: &mut Transaction<'_>,
		ringbuffer: &RingBuffer,
		partition: Option<Partition>,
	) -> Result<()> {
		txn.remove(vec![(RingBufferMetadataKey::of(ringbuffer.id, partition).into(), None)])
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use super::*;
	use crate::test_utils::ensure_test_ringbuffer;

	#[test]
	fn test_saved_metadata_is_found_under_its_own_partition_only() {
		// A save that ignored the partition would hand one partition's slot range to another.
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);
		let metadata = RingBufferMetadata {
			base_rn: 40,
			next_sequence: 7,
			count: 5,
		};
		let mut tx = Transaction::Admin(&mut txn);
		CatalogStore::save_partition_metadata(&mut tx, &ringbuffer, Some(Partition(9)), &metadata).unwrap();

		assert_eq!(
			CatalogStore::find_partition_metadata(&mut tx, &ringbuffer, Some(Partition(9))).unwrap(),
			Some(metadata)
		);
		assert_eq!(
			CatalogStore::find_partition_metadata(&mut tx, &ringbuffer, Some(Partition(8))).unwrap(),
			None
		);
	}

	#[test]
	fn test_removed_metadata_disappears_from_the_partition_list() {
		// A lingering metadata row would make scans list a partition that holds no rows.
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);
		let mut tx = Transaction::Admin(&mut txn);
		let metadata = RingBufferMetadata::reserved(10);
		CatalogStore::save_partition_metadata(&mut tx, &ringbuffer, Some(Partition(1)), &metadata).unwrap();
		CatalogStore::remove_partition_metadata(&mut tx, &ringbuffer, Some(Partition(1))).unwrap();

		let listed = CatalogStore::list_ringbuffer_partitions(&mut tx, ringbuffer.id.into()).unwrap();
		assert!(listed.iter().all(|entry| entry.partition != Some(Partition(1))));
	}
}
