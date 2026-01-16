// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{
	id::RingBufferId,
	ringbuffer::{RingBufferDef, RingBufferMetadata},
};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{error::Error, internal};

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_ringbuffer(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RingBufferDef> {
		Self::find_ringbuffer(rx, ringbuffer)?.ok_or_else(|| {
			Error(internal!(
				"Ring buffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				ringbuffer
			))
		})
	}

	pub fn get_ringbuffer_metadata(
		rx: &mut impl IntoStandardTransaction,
		ringbuffer: RingBufferId,
	) -> crate::Result<RingBufferMetadata> {
		Self::find_ringbuffer_metadata(rx, ringbuffer)?.ok_or_else(|| {
			Error(internal!(
				"Ring buffer metadata for ID {:?} not found. This indicates a critical catalog inconsistency.",
				ringbuffer
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::RingBufferId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[test]
	fn test_get_ringbuffer_exists() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let result = CatalogStore::get_ringbuffer(&mut txn, ringbuffer.id).unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.name, ringbuffer.name);
	}

	#[test]
	fn test_get_ringbuffer_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::get_ringbuffer(&mut txn, RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_ringbuffer_metadata_exists() {
		let mut txn = create_test_command_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let result = CatalogStore::get_ringbuffer_metadata(&mut txn, ringbuffer.id).unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.capacity, ringbuffer.capacity);
	}

	#[test]
	fn test_get_ringbuffer_metadata_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::get_ringbuffer_metadata(&mut txn, RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found"));
	}
}
