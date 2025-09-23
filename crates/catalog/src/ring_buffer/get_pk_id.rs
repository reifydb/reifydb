// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{Key, PrimaryKeyId, QueryTransaction, RingBufferId, RingBufferKey};

use crate::{CatalogStore, ring_buffer::layout::ring_buffer};

impl CatalogStore {
	/// Get the primary key ID for a ring buffer
	/// Returns None if the ring buffer doesn't exist or has no primary key
	pub fn get_ring_buffer_pk_id(
		rx: &mut impl QueryTransaction,
		ring_buffer_id: RingBufferId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let multi = match rx.get(&Key::RingBuffer(RingBufferKey::new(ring_buffer_id)).encode())? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = ring_buffer::LAYOUT.get_u64(&multi.row, ring_buffer::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimaryKeyId, RingBufferId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ring_buffer};

	#[test]
	fn test_get_ring_buffer_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let pk_id = CatalogStore::get_ring_buffer_pk_id(&mut txn, ring_buffer.id).unwrap();

		assert_eq!(pk_id, None);
	}

	#[test]
	fn test_get_ring_buffer_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		// Set primary key
		let pk_id = PrimaryKeyId(42);
		CatalogStore::set_ring_buffer_primary_key(&mut txn, ring_buffer.id, pk_id).unwrap();

		// Get and verify
		let retrieved_pk = CatalogStore::get_ring_buffer_pk_id(&mut txn, ring_buffer.id).unwrap();

		assert_eq!(retrieved_pk, Some(pk_id));
	}

	#[test]
	fn test_get_ring_buffer_pk_id_nonexistent_ring_buffer() {
		let mut txn = create_test_command_transaction();

		let pk_id = CatalogStore::get_ring_buffer_pk_id(&mut txn, RingBufferId(999)).unwrap();

		assert_eq!(pk_id, None);
	}
}
