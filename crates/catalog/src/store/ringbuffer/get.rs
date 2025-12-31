// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{RingBufferDef, RingBufferId, RingBufferMetadata};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{Error, internal};

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_ringbuffer(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		Self::find_ringbuffer(rx, ringbuffer).await?.ok_or_else(|| {
			Error(internal!(
				"Ring buffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				ringbuffer
			))
		})
	}

	pub async fn get_ringbuffer_metadata(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RingBufferMetadata> {
		Self::find_ringbuffer_metadata(rx, ringbuffer).await?.ok_or_else(|| {
			Error(internal!(
				"Ring buffer metadata for ID {:?} not found. This indicates a critical catalog inconsistency.",
				ringbuffer
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::RingBufferId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[tokio::test]
	async fn test_get_ringbuffer_exists() {
		let mut txn = create_test_command_transaction().await;
		let ringbuffer = ensure_test_ringbuffer(&mut txn).await;

		let result = CatalogStore::get_ringbuffer(&mut txn, ringbuffer.id).await.unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.name, ringbuffer.name);
	}

	#[tokio::test]
	async fn test_get_ringbuffer_not_exists() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::get_ringbuffer(&mut txn, RingBufferId(999)).await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[tokio::test]
	async fn test_get_ringbuffer_metadata_exists() {
		let mut txn = create_test_command_transaction().await;
		let ringbuffer = ensure_test_ringbuffer(&mut txn).await;

		let result = CatalogStore::get_ringbuffer_metadata(&mut txn, ringbuffer.id).await.unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.capacity, ringbuffer.capacity);
	}

	#[tokio::test]
	async fn test_get_ringbuffer_metadata_not_exists() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::get_ringbuffer_metadata(&mut txn, RingBufferId(999)).await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found"));
	}
}
