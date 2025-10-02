// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, Key, PrimaryKeyId, RingBufferId, RingBufferKey},
	return_internal_error,
};

use crate::{CatalogStore, ring_buffer::layout::ring_buffer};

impl CatalogStore {
	/// Set the primary key ID for a ring buffer
	/// Returns an internal error if the ring buffer doesn't exist
	pub fn set_ring_buffer_primary_key(
		txn: &mut impl CommandTransaction,
		ring_buffer_id: RingBufferId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let multi = match txn.get(&Key::RingBuffer(RingBufferKey::new(ring_buffer_id)).encode())? {
			Some(v) => v,
			None => return_internal_error!(format!(
				"Ring buffer with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				ring_buffer_id.0
			)),
		};

		let mut updated_row = multi.values.clone();
		ring_buffer::LAYOUT.set_u64(&mut updated_row, ring_buffer::PRIMARY_KEY, primary_key_id.0);

		txn.set(&Key::RingBuffer(RingBufferKey::new(ring_buffer_id)).encode(), updated_row)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimaryKeyId, RingBufferId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ring_buffer};

	#[test]
	fn test_set_ring_buffer_primary_key() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		// Set primary key
		let pk_id = PrimaryKeyId(100);
		CatalogStore::set_ring_buffer_primary_key(&mut txn, ring_buffer.id, pk_id).unwrap();

		// Verify it was set
		let retrieved_pk = CatalogStore::get_ring_buffer_pk_id(&mut txn, ring_buffer.id).unwrap();
		assert_eq!(retrieved_pk, Some(pk_id));
	}

	#[test]
	fn test_set_ring_buffer_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::set_ring_buffer_primary_key(&mut txn, RingBufferId(999), PrimaryKeyId(1));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Ring buffer with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_set_ring_buffer_primary_key_overwrites() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		// Set first primary key
		let pk_id1 = PrimaryKeyId(100);
		CatalogStore::set_ring_buffer_primary_key(&mut txn, ring_buffer.id, pk_id1).unwrap();

		// Set second primary key (should overwrite)
		let pk_id2 = PrimaryKeyId(200);
		CatalogStore::set_ring_buffer_primary_key(&mut txn, ring_buffer.id, pk_id2).unwrap();

		// Verify second one is set
		let retrieved_pk = CatalogStore::get_ring_buffer_pk_id(&mut txn, ring_buffer.id).unwrap();
		assert_eq!(retrieved_pk, Some(pk_id2));
	}
}
