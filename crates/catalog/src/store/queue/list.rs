// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, QueueId},
		queue::Queue,
	},
	key::{Key, queue::QueueKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn list_queues_all(rx: &mut Transaction<'_>) -> Result<Vec<Queue>> {
		let mut queue_ids: Vec<QueueId> = Vec::new();
		{
			let stream = rx.range(QueueKey::full_scan(), RangeScope::All, 1024)?;

			for entry in stream {
				let entry = entry?;
				if let Some(Key::Queue(queue_key)) = Key::decode(&entry.key) {
					queue_ids.push(queue_key.queue);
				}
			}
		}

		let mut result = Vec::with_capacity(queue_ids.len());
		for queue_id in queue_ids {
			result.push(Self::get_queue(rx, queue_id)?);
		}

		Ok(result)
	}

	pub(crate) fn list_queues(rx: &mut Transaction<'_>, namespace: NamespaceId) -> Result<Vec<Queue>> {
		Ok(Self::list_queues_all(rx)?.into_iter().filter(|queue| queue.namespace == namespace).collect())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::NamespaceId,
		queue::{QueueRetention, QueueRetry},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::{namespace::create::NamespaceToCreate, queue::create::QueueToCreate},
		test_utils::ensure_test_namespace,
	};

	fn to_create(name: &str, namespace: NamespaceId) -> QueueToCreate {
		QueueToCreate {
			name: Fragment::internal(name),
			namespace,
			columns: vec![],
			partitions: 16,
			ordered_by: None,
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			underlying: false,
		}
	}

	#[test]
	fn test_list_queues_empty() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);

		let queues = CatalogStore::list_queues_all(&mut Transaction::Admin(&mut txn)).unwrap();

		assert_eq!(queues.len(), 0);
	}

	/// Listing per namespace must not leak queues from a sibling namespace, or
	/// system::queues would show foreign definitions.
	#[test]
	fn test_list_queues_filters_by_namespace() {
		let mut txn = create_test_admin_transaction();
		let first = ensure_test_namespace(&mut txn);
		let second = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "other".to_string(),
				local_name: "other".to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.unwrap();

		CatalogStore::create_queue(&mut txn, to_create("here", first.id())).unwrap();
		CatalogStore::create_queue(&mut txn, to_create("there", second.id())).unwrap();

		let all = CatalogStore::list_queues_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all.len(), 2);

		let scoped = CatalogStore::list_queues(&mut Transaction::Admin(&mut txn), first.id()).unwrap();
		assert_eq!(scoped.len(), 1);
		assert_eq!(scoped[0].name, "here");
	}
}
