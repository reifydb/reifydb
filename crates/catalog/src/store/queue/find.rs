// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, QueueId},
		queue::{Queue, QueueRetention, QueueRetry},
	},
	key::{namespace_queue::NamespaceQueueKey, queue::QueueKey},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::queue::shape::{queue, queue_namespace},
};

impl CatalogStore {
	pub(crate) fn find_queue(rx: &mut Transaction<'_>, queue_id: QueueId) -> Result<Option<Queue>> {
		let Some(multi) = rx.get(&QueueKey::encoded(queue_id))? else {
			return Ok(None);
		};

		let row = multi.row;
		let id = QueueId(queue::SHAPE.get_u64(&row, queue::ID));
		let namespace = NamespaceId(queue::SHAPE.get_u64(&row, queue::NAMESPACE));
		let name = queue::SHAPE.get_utf8(&row, queue::NAME).to_string();
		let partitions = queue::SHAPE.get_u16(&row, queue::PARTITIONS);

		let ordered_by_str = queue::SHAPE.get_utf8(&row, queue::ORDERED_BY);
		let ordered_by = if ordered_by_str.is_empty() {
			None
		} else {
			Some(ordered_by_str.to_string())
		};

		let retention = QueueRetention {
			done: queue::SHAPE.try_get_duration(&row, queue::RETENTION_DONE),
		};
		let retry = QueueRetry {
			attempts: queue::SHAPE.get_u32(&row, queue::RETRY_ATTEMPTS),
			backoff: queue::SHAPE.get_duration(&row, queue::RETRY_BACKOFF),
		};
		let underlying = queue::SHAPE.get_u8(&row, queue::UNDERLYING) != 0;

		Ok(Some(Queue {
			id,
			namespace,
			name,
			columns: Self::list_columns(rx, id)?,
			partitions,
			ordered_by,
			retention,
			retry,
			underlying,
		}))
	}

	pub(crate) fn find_queue_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<Queue>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceQueueKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_queue = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let row = &multi.row;
			let queue_name = queue_namespace::SHAPE.get_utf8(row, queue_namespace::NAME);
			if name == queue_name {
				found_queue = Some(QueueId(queue_namespace::SHAPE.get_u64(row, queue_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(queue_id) = found_queue else {
			return Ok(None);
		};

		Ok(Some(Self::get_queue(rx, queue_id)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		id::{NamespaceId, QueueId},
		queue::{QueueRetention, QueueRetry},
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::fragment::Fragment;

	use crate::{CatalogStore, store::queue::create::QueueToCreate, test_utils::ensure_test_namespace};

	fn create(txn: &mut AdminTransaction, name: &str) -> QueueId {
		let namespace = ensure_test_namespace(txn);
		CatalogStore::create_queue(
			txn,
			QueueToCreate {
				name: Fragment::internal(name),
				namespace: namespace.id(),
				columns: vec![],
				partitions: 16,
				ordered_by: None,
				retention: QueueRetention::default(),
				retry: QueueRetry::default(),
				underlying: false,
			},
		)
		.unwrap()
		.id
	}

	#[test]
	fn test_find_queue_by_id() {
		let mut txn = create_test_admin_transaction();
		let id = create(&mut txn, "jobs");

		let found = CatalogStore::find_queue(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();

		assert_eq!(found.id, id);
		assert_eq!(found.name, "jobs");
	}

	#[test]
	fn test_find_queue_unknown_id_is_none() {
		let mut txn = create_test_admin_transaction();

		let found = CatalogStore::find_queue(&mut Transaction::Admin(&mut txn), QueueId(999)).unwrap();

		assert!(found.is_none());
	}

	/// Name lookup is namespace-scoped: a queue must not be findable from a
	/// namespace that does not contain it.
	#[test]
	fn test_find_queue_by_name_is_namespace_scoped() {
		let mut txn = create_test_admin_transaction();
		let id = create(&mut txn, "jobs");
		let namespace = ensure_test_namespace(&mut txn);

		let found = CatalogStore::find_queue_by_name(&mut Transaction::Admin(&mut txn), namespace.id(), "jobs")
			.unwrap()
			.unwrap();
		assert_eq!(found.id, id);

		let elsewhere =
			CatalogStore::find_queue_by_name(&mut Transaction::Admin(&mut txn), NamespaceId(9999), "jobs")
				.unwrap();
		assert!(elsewhere.is_none());
	}
}
