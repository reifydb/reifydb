// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{CommandTransaction, EncodableKey, RingBufferMetadata, RingBufferMetadataKey};

use crate::{CatalogStore, ring_buffer::layout::ring_buffer_metadata};

impl CatalogStore {
	pub fn update_ring_buffer_metadata(
		txn: &mut impl CommandTransaction,
		metadata: RingBufferMetadata,
	) -> crate::Result<()> {
		let mut row = ring_buffer_metadata::LAYOSVT.allocate_row();
		ring_buffer_metadata::LAYOSVT.set_u64(&mut row, ring_buffer_metadata::ID, metadata.id);
		ring_buffer_metadata::LAYOSVT.set_u64(&mut row, ring_buffer_metadata::CAPACITY, metadata.capacity);
		ring_buffer_metadata::LAYOSVT.set_u64(&mut row, ring_buffer_metadata::HEAD, metadata.head);
		ring_buffer_metadata::LAYOSVT.set_u64(&mut row, ring_buffer_metadata::TAIL, metadata.tail);
		ring_buffer_metadata::LAYOSVT.set_u64(&mut row, ring_buffer_metadata::COUNT, metadata.count);

		let key = RingBufferMetadataKey::new(metadata.id);
		txn.set(&key.encode(), row)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::*;
	use crate::test_utils::ensure_test_ring_buffer;

	#[test]
	fn test_update_ring_buffer_metadata() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		// Get initial metadata
		let mut metadata = CatalogStore::find_ring_buffer_metadata(&mut txn, ring_buffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(metadata.count, 0);
		assert_eq!(metadata.head, 0);
		assert_eq!(metadata.tail, 0);

		// Update metadata
		metadata.count = 5;
		metadata.head = 2;
		metadata.tail = 7;

		CatalogStore::update_ring_buffer_metadata(&mut txn, metadata.clone()).unwrap();

		// Verify update
		let updated = CatalogStore::find_ring_buffer_metadata(&mut txn, ring_buffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(updated.count, 5);
		assert_eq!(updated.head, 2);
		assert_eq!(updated.tail, 7);
		assert_eq!(updated.capacity, metadata.capacity);
	}

	#[test]
	fn test_update_ring_buffer_metadata_wrap_around() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let mut metadata = CatalogStore::find_ring_buffer_metadata(&mut txn, ring_buffer.id)
			.unwrap()
			.expect("Metadata should exist");

		// Simulate wrap-around scenario
		metadata.count = metadata.capacity;
		metadata.head = metadata.capacity - 1;
		metadata.tail = 0;

		CatalogStore::update_ring_buffer_metadata(&mut txn, metadata.clone()).unwrap();

		let updated = CatalogStore::find_ring_buffer_metadata(&mut txn, ring_buffer.id)
			.unwrap()
			.expect("Metadata should exist");

		assert_eq!(updated.count, metadata.capacity);
		assert_eq!(updated.head, metadata.capacity - 1);
		assert_eq!(updated.tail, 0);
	}
}
