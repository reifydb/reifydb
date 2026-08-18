// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::QueueId,
	key::{namespace_queue::NamespaceQueueKey, queue::QueueKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::object::drop::drop_object_metadata};

impl CatalogStore {
	pub(crate) fn drop_queue(txn: &mut AdminTransaction, queue: QueueId) -> Result<()> {
		if let Some(queue_def) = Self::find_queue(&mut Transaction::Admin(&mut *txn), queue)? {
			txn.remove(&NamespaceQueueKey::encoded(queue_def.namespace, queue))?;
		}

		drop_object_metadata(txn, queue.into(), None)?;

		txn.remove(&QueueKey::encoded(queue))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			id::QueueId,
			queue::{QueueDispatch, QueueRetention, QueueRetry},
		},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::queue::create::{QueueColumnToCreate, QueueToCreate},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_drop_queue() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			QueueToCreate {
				name: Fragment::internal("jobs"),
				namespace: namespace.id(),
				columns: vec![],
				dispatch: QueueDispatch::Fifo {
					partitions: 16,
					ordered_by: None,
				},
				retention: QueueRetention::default(),
				retry: QueueRetry::default(),
				deduplicate: None,
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		CatalogStore::drop_queue(&mut txn, created.id).unwrap();

		assert!(CatalogStore::find_queue(&mut Transaction::Admin(&mut txn), created.id).unwrap().is_none());
	}

	#[test]
	fn test_drop_nonexistent_queue_is_a_noop() {
		let mut txn = create_test_admin_transaction();

		assert!(CatalogStore::drop_queue(&mut txn, QueueId(999999)).is_ok());
	}

	#[test]
	fn test_drop_queue_cleans_up_columns() {
		// Orphaned column metadata would attach itself to whatever object later reuses the id.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			QueueToCreate {
				name: Fragment::internal("jobs"),
				namespace: namespace.id(),
				columns: vec![QueueColumnToCreate {
					name: Fragment::internal("payload"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				}],
				dispatch: QueueDispatch::Fifo {
					partitions: 16,
					ordered_by: None,
				},
				retention: QueueRetention::default(),
				retry: QueueRetry::default(),
				deduplicate: None,
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), created.id).unwrap().len(), 1);

		CatalogStore::drop_queue(&mut txn, created.id).unwrap();

		assert!(CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), created.id).unwrap().is_empty());
	}

	#[test]
	fn test_dropped_queue_name_is_reusable() {
		// If the name link outlives the drop the name stays taken and the queue can never
		// be recreated.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = || QueueToCreate {
			name: Fragment::internal("jobs"),
			namespace: namespace.id(),
			columns: vec![],
			dispatch: QueueDispatch::Fifo {
				partitions: 16,
				ordered_by: None,
			},
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			deduplicate: None,
			time: TimeSource::Processing,
		};

		let first = CatalogStore::create_queue(&mut txn, to_create()).unwrap();
		CatalogStore::drop_queue(&mut txn, first.id).unwrap();

		let second = CatalogStore::create_queue(&mut txn, to_create()).unwrap();
		assert_ne!(second.id, first.id);

		let found = CatalogStore::find_queue_by_name(&mut Transaction::Admin(&mut txn), namespace.id(), "jobs")
			.unwrap()
			.unwrap();
		assert_eq!(found.id, second.id);
	}
}
