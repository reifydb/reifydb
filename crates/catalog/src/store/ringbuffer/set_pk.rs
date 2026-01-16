// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, RingBufferId},
	key::ringbuffer::RingBufferKey,
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::return_internal_error;

use crate::{CatalogStore, store::ringbuffer::layout::ringbuffer};

impl CatalogStore {
	/// Set the primary key ID for a ring buffer
	/// Returns an internal error if the ring buffer doesn't exist
	pub fn set_ringbuffer_primary_key(
		txn: &mut StandardCommandTransaction,
		ringbuffer_id: RingBufferId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let multi = match txn.get(&RingBufferKey::encoded(ringbuffer_id))? {
			Some(v) => v,
			None => return_internal_error!(format!(
				"Ring buffer with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				ringbuffer_id.0
			)),
		};

		let mut updated_row = multi.values.clone();
		ringbuffer::LAYOUT.set_u64(&mut updated_row, ringbuffer::PRIMARY_KEY, primary_key_id.0);

		txn.set(&RingBufferKey::encoded(ringbuffer_id), updated_row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{PrimaryKeyId, RingBufferId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[test]
	fn test_set_ringbuffer_primary_key() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		// Set primary key
		let pk_id = PrimaryKeyId(100);
		CatalogStore::set_ringbuffer_primary_key(&mut txn, ringbuffer.id, pk_id).unwrap();

		// Verify it was set
		let retrieved_pk = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).unwrap();
		assert_eq!(retrieved_pk, Some(pk_id));
	}

	#[test]
	fn test_set_ringbuffer_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::set_ringbuffer_primary_key(&mut txn, RingBufferId(999), PrimaryKeyId(1));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("Ring buffer with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}

	#[test]
	fn test_set_ringbuffer_primary_key_overwrites() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		// Set first primary key
		let pk_id1 = PrimaryKeyId(100);
		CatalogStore::set_ringbuffer_primary_key(&mut txn, ringbuffer.id, pk_id1).unwrap();

		// Set second primary key (should overwrite)
		let pk_id2 = PrimaryKeyId(200);
		CatalogStore::set_ringbuffer_primary_key(&mut txn, ringbuffer.id, pk_id2).unwrap();

		// Verify second one is set
		let retrieved_pk = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).unwrap();
		assert_eq!(retrieved_pk, Some(pk_id2));
	}
}
