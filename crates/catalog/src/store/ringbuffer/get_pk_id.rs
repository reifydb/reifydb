// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, RingBufferId},
	key::ringbuffer::RingBufferKey,
};
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{CatalogStore, store::ringbuffer::schema::ringbuffer};

impl CatalogStore {
	/// Get the primary key ID for a ring buffer
	/// Returns None if the ring buffer doesn't exist or has no primary key
	pub fn get_ringbuffer_pk_id(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer_id: RingBufferId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let mut txn = rx.into_standard_transaction();
		let multi = match txn.get(&RingBufferKey::encoded(ringbuffer_id))? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = ringbuffer::SCHEMA.get_u64(&multi.values, ringbuffer::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{PrimaryKeyId, RingBufferId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[test]
	fn test_get_ringbuffer_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let pk_id = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).unwrap();

		assert_eq!(pk_id, None);
	}

	#[test]
	fn test_get_ringbuffer_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		// Set primary key
		let pk_id = PrimaryKeyId(42);
		CatalogStore::set_ringbuffer_primary_key(&mut txn, ringbuffer.id, pk_id).unwrap();

		// Get and verify
		let retrieved_pk = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).unwrap();

		assert_eq!(retrieved_pk, Some(pk_id));
	}

	#[test]
	fn test_get_ringbuffer_pk_id_nonexistent_ringbuffer() {
		let mut txn = create_test_command_transaction();

		let pk_id = CatalogStore::get_ringbuffer_pk_id(&mut txn, RingBufferId(999)).unwrap();

		assert_eq!(pk_id, None);
	}
}
