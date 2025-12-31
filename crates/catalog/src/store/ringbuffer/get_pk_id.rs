// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{PrimaryKeyId, RingBufferId, RingBufferKey};
use reifydb_transaction::IntoStandardTransaction;

use crate::{CatalogStore, store::ringbuffer::layout::ringbuffer};

impl CatalogStore {
	/// Get the primary key ID for a ring buffer
	/// Returns None if the ring buffer doesn't exist or has no primary key
	pub async fn get_ringbuffer_pk_id(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer_id: RingBufferId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let mut txn = rx.into_standard_transaction();
		let multi = match txn.get(&RingBufferKey::encoded(ringbuffer_id)).await? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = ringbuffer::LAYOUT.get_u64(&multi.values, ringbuffer::PRIMARY_KEY);

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

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[tokio::test]
	async fn test_get_ringbuffer_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction().await;
		let ringbuffer = ensure_test_ringbuffer(&mut txn).await;

		let pk_id = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).await.unwrap();

		assert_eq!(pk_id, None);
	}

	#[tokio::test]
	async fn test_get_ringbuffer_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction().await;
		let ringbuffer = ensure_test_ringbuffer(&mut txn).await;

		// Set primary key
		let pk_id = PrimaryKeyId(42);
		CatalogStore::set_ringbuffer_primary_key(&mut txn, ringbuffer.id, pk_id).await.unwrap();

		// Get and verify
		let retrieved_pk = CatalogStore::get_ringbuffer_pk_id(&mut txn, ringbuffer.id).await.unwrap();

		assert_eq!(retrieved_pk, Some(pk_id));
	}

	#[tokio::test]
	async fn test_get_ringbuffer_pk_id_nonexistent_ringbuffer() {
		let mut txn = create_test_command_transaction().await;

		let pk_id = CatalogStore::get_ringbuffer_pk_id(&mut txn, RingBufferId(999)).await.unwrap();

		assert_eq!(pk_id, None);
	}
}
