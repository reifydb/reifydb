// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{QueryTransaction, RingBufferDef, RingBufferId, RingBufferMetadata};
use reifydb_type::{Error, internal_error};

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_ring_buffer(
		rx: &mut impl QueryTransaction,
		ring_buffer: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		Self::find_ring_buffer(rx, ring_buffer)?.ok_or_else(|| {
			Error(internal_error!(
				"Ring buffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				ring_buffer
			))
		})
	}

	pub fn get_ring_buffer_metadata(
		rx: &mut impl QueryTransaction,
		ring_buffer: RingBufferId,
	) -> crate::Result<RingBufferMetadata> {
		Self::find_ring_buffer_metadata(rx, ring_buffer)?.ok_or_else(|| {
			Error(internal_error!(
				"Ring buffer metadata for ID {:?} not found. This indicates a critical catalog inconsistency.",
				ring_buffer
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::RingBufferId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ring_buffer};

	#[test]
	fn test_get_ring_buffer_exists() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let result = CatalogStore::get_ring_buffer(&mut txn, ring_buffer.id).unwrap();

		assert_eq!(result.id, ring_buffer.id);
		assert_eq!(result.name, ring_buffer.name);
	}

	#[test]
	fn test_get_ring_buffer_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::get_ring_buffer(&mut txn, RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_ring_buffer_metadata_exists() {
		let mut txn = create_test_command_transaction();
		let ring_buffer = ensure_test_ring_buffer(&mut txn);

		let result = CatalogStore::get_ring_buffer_metadata(&mut txn, ring_buffer.id).unwrap();

		assert_eq!(result.id, ring_buffer.id);
		assert_eq!(result.capacity, ring_buffer.capacity);
	}

	#[test]
	fn test_get_ring_buffer_metadata_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::get_ring_buffer_metadata(&mut txn, RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found"));
	}
}
