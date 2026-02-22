// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::RingBufferId,
	key::{
		namespace_ringbuffer::NamespaceRingBufferKey,
		ringbuffer::{RingBufferKey, RingBufferMetadataKey},
	},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, store::primitive::drop::drop_primitive_metadata};

impl CatalogStore {
	pub(crate) fn drop_ringbuffer(txn: &mut AdminTransaction, ringbuffer: RingBufferId) -> crate::Result<()> {
		// First, find the ringbuffer to get its namespace and primary key
		let pk_id = if let Some(ringbuffer_def) =
			Self::find_ringbuffer(&mut Transaction::Admin(&mut *txn), ringbuffer)?
		{
			// Remove the namespace-ringbuffer link (secondary index)
			txn.remove(&NamespaceRingBufferKey::encoded(ringbuffer_def.namespace, ringbuffer))?;
			ringbuffer_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		// Clean up all associated metadata (columns, policies, sequences, pk, retention)
		drop_primitive_metadata(txn, ringbuffer.into(), pk_id)?;

		// Remove the ringbuffer metadata
		txn.remove(&RingBufferMetadataKey::encoded(ringbuffer))?;

		// Remove the ringbuffer definition
		txn.remove(&RingBufferKey::encoded(ringbuffer))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{id::RingBufferId, primitive::PrimitiveId},
		retention::RetentionPolicy,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::{
			retention_policy::create::create_primitive_retention_policy,
			ringbuffer::create::RingBufferColumnToCreate,
		},
		test_utils::{create_ringbuffer, ensure_test_namespace, ensure_test_ringbuffer},
	};

	#[test]
	fn test_drop_ringbuffer() {
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_ringbuffer(&mut txn);

		// Verify it exists
		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		// Drop it
		CatalogStore::drop_ringbuffer(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_ringbuffer() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent ringbuffer should not error
		let non_existent = RingBufferId(999999);
		let result = CatalogStore::drop_ringbuffer(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_ringbuffer_cleans_up_metadata() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		// Create a ringbuffer with columns
		let rb = create_ringbuffer(
			&mut txn,
			"test_namespace",
			"meta_rb",
			100,
			&[
				RingBufferColumnToCreate {
					name: Fragment::internal("col_a"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Int4),
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: Fragment::internal("col_b"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					policies: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
		);

		// Add retention policy
		create_primitive_retention_policy(
			&mut txn,
			PrimitiveId::RingBuffer(rb.id),
			&RetentionPolicy::KeepForever,
		)
		.unwrap();

		// Verify columns exist before drop
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Verify retention policy exists before drop
		let policy = CatalogStore::find_primitive_retention_policy(
			&mut Transaction::Admin(&mut txn),
			PrimitiveId::RingBuffer(rb.id),
		)
		.unwrap();
		assert!(policy.is_some());

		// Drop the ringbuffer
		CatalogStore::drop_ringbuffer(&mut txn, rb.id).unwrap();

		// Verify columns are cleaned up
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert!(columns.is_empty());

		// Verify retention policy is cleaned up
		let policy = CatalogStore::find_primitive_retention_policy(
			&mut Transaction::Admin(&mut txn),
			PrimitiveId::RingBuffer(rb.id),
		)
		.unwrap();
		assert!(policy.is_none());

		// Verify ringbuffer itself is gone
		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert!(found.is_none());
	}
}
