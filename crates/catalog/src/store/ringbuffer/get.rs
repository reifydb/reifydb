// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::RingBufferId,
		ringbuffer::{RingBufferDef, RingBufferMetadata},
	},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_ringbuffer(rx: &mut Transaction<'_>, ringbuffer: RingBufferId) -> Result<RingBufferDef> {
		Self::find_ringbuffer(rx, ringbuffer)?.ok_or_else(|| {
			Error(internal!(
				"Ring buffer with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				ringbuffer
			))
		})
	}

	pub(crate) fn get_ringbuffer_metadata(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
	) -> Result<RingBufferMetadata> {
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
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, test_utils::ensure_test_ringbuffer};

	#[test]
	fn test_get_ringbuffer_exists() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let result = CatalogStore::get_ringbuffer(&mut Transaction::Admin(&mut txn), ringbuffer.id).unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.name, ringbuffer.name);
	}

	#[test]
	fn test_get_ringbuffer_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::get_ringbuffer(&mut Transaction::Admin(&mut txn), RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}

	#[test]
	fn test_get_ringbuffer_metadata_exists() {
		let mut txn = create_test_admin_transaction();
		let ringbuffer = ensure_test_ringbuffer(&mut txn);

		let result = CatalogStore::get_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), ringbuffer.id)
			.unwrap();

		assert_eq!(result.id, ringbuffer.id);
		assert_eq!(result.capacity, ringbuffer.capacity);
	}

	#[test]
	fn test_get_ringbuffer_metadata_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result =
			CatalogStore::get_ringbuffer_metadata(&mut Transaction::Admin(&mut txn), RingBufferId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("RingBufferId(999)"));
		assert!(err.message.contains("not found"));
	}
}
