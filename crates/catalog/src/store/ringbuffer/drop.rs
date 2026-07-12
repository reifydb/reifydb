// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::RingBufferId,
	key::{namespace_ringbuffer::NamespaceRingBufferKey, ringbuffer::RingBufferKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::shape::drop::drop_shape_metadata};

impl CatalogStore {
	pub(crate) fn drop_ringbuffer(txn: &mut AdminTransaction, ringbuffer: RingBufferId) -> Result<()> {
		let pk_id = if let Some(ringbuffer_def) =
			Self::find_ringbuffer(&mut Transaction::Admin(&mut *txn), ringbuffer)?
		{
			txn.remove(&NamespaceRingBufferKey::encoded(ringbuffer_def.namespace, ringbuffer))?;

			let partitions =
				Self::list_ringbuffer_partitions(&mut Transaction::Admin(&mut *txn), &ringbuffer_def)?;
			for partition in partitions {
				Self::remove_partition_metadata(
					&mut Transaction::Admin(&mut *txn),
					&ringbuffer_def,
					&partition.partition_values,
				)?;
			}

			ringbuffer_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		drop_shape_metadata(txn, ringbuffer.into(), pk_id)?;

		txn.remove(&RingBufferKey::encoded(ringbuffer))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{id::RingBufferId, ringbuffer::RingBufferMetadata, shape::ShapeId},
		key::ringbuffer::RingBufferMetadataKey,
		retention::RetentionStrategy,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
	use reifydb_value::{
		fragment::Fragment,
		value::{Value, constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::{
			retention_strategy::create::create_shape_retention_strategy,
			ringbuffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
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
					constraint: TypeConstraint::unconstrained(ValueType::Int4),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				RingBufferColumnToCreate {
					name: Fragment::internal("col_b"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
		);

		// Add retention strategy
		create_shape_retention_strategy(&mut txn, ShapeId::RingBuffer(rb.id), &RetentionStrategy::KeepForever)
			.unwrap();

		// Verify columns exist before drop
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Verify retention strategy exists before drop
		let policy = CatalogStore::find_shape_retention_strategy(
			&mut Transaction::Admin(&mut txn),
			ShapeId::RingBuffer(rb.id),
		)
		.unwrap();
		assert!(policy.is_some());

		// Drop the ringbuffer
		CatalogStore::drop_ringbuffer(&mut txn, rb.id).unwrap();

		// Verify columns are cleaned up
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert!(columns.is_empty());

		// Verify retention strategy is cleaned up
		let policy = CatalogStore::find_shape_retention_strategy(
			&mut Transaction::Admin(&mut txn),
			ShapeId::RingBuffer(rb.id),
		)
		.unwrap();
		assert!(policy.is_none());

		// Verify ringbuffer itself is gone
		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), rb.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_ringbuffer_removes_all_partition_metadata() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let rb = CatalogStore::create_ringbuffer(
			&mut txn,
			RingBufferToCreate {
				name: Fragment::internal("partitioned_rb"),
				namespace: namespace.id(),
				columns: vec![RingBufferColumnToCreate {
					name: Fragment::internal("region"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				}],
				capacity: 10,
				partition_by: vec!["region".to_string()],
				underlying: false,
			},
		)
		.unwrap();

		// Simulate two distinct partitions having received rows.
		for region in ["us", "eu"] {
			let partition_key = vec![Value::Utf8(region.to_string())];
			let metadata = RingBufferMetadata {
				id: rb.id,
				capacity: rb.capacity,
				count: 3,
				head: 1,
				tail: 4,
			};
			CatalogStore::update_ringbuffer_partition_metadata_txn(
				&mut Transaction::Admin(&mut txn),
				rb.id,
				&partition_key,
				&metadata,
			)
			.unwrap();
		}

		let before = CatalogStore::list_ringbuffer_partition_metadata(&mut Transaction::Admin(&mut txn), &rb)
			.unwrap();
		assert_eq!(before.len(), 2);

		CatalogStore::drop_ringbuffer(&mut txn, rb.id).unwrap();

		// The ringbuffer definition is gone, so scan the raw metadata keyspace for this id directly
		// rather than going through list_ringbuffer_partition_metadata (which needs a RingBuffer).
		let range = RingBufferMetadataKey::full_scan_for_ringbuffer(rb.id);
		let remaining: Vec<_> =
			Transaction::Admin(&mut txn).range(range, RangeScope::All, 4096).unwrap().collect();
		assert!(remaining.is_empty(), "expected no orphaned RingBufferMetadataKey entries after drop");
	}
}
