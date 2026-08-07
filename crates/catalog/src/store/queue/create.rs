// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::TimeSource,
	interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, NamespaceId, QueueId},
		property::ColumnPropertyKind,
		queue::{Queue, QueueDeduplicate, QueueDispatch, QueueRetention, QueueRetry},
	},
	key::{namespace_queue::NamespaceQueueKey, queue::QueueKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, dictionary::DictionaryId, duration::Duration},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		column::create::ColumnToCreate,
		queue::shape::{encode_deduplicate, encode_dispatch, queue, queue_namespace},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct QueueColumnToCreate {
	pub name: Fragment,
	pub fragment: Fragment,
	pub constraint: TypeConstraint,
	pub properties: Vec<ColumnPropertyKind>,
	pub auto_increment: bool,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct QueueToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub columns: Vec<QueueColumnToCreate>,
	pub dispatch: QueueDispatch,
	pub deduplicate: Option<QueueDeduplicate>,
	pub retention: QueueRetention,
	pub retry: QueueRetry,
	pub underlying: bool,
	pub time: TimeSource,
}

use crate::store::time_source::write_time_source;

impl CatalogStore {
	pub(crate) fn create_queue(txn: &mut AdminTransaction, to_create: QueueToCreate) -> Result<Queue> {
		let namespace_id = to_create.namespace;
		Self::reject_existing_queue(txn, namespace_id, &to_create.name)?;

		let queue_id = SystemSequence::next_queue_id(txn)?;

		Self::store_queue(txn, queue_id, namespace_id, &to_create)?;
		Self::link_queue_to_namespace(txn, namespace_id, queue_id, to_create.name.text())?;
		Self::insert_queue_columns(txn, queue_id, to_create)?;

		Self::get_queue(&mut Transaction::Admin(&mut *txn), queue_id)
	}

	pub(crate) fn create_queue_with_id(
		txn: &mut AdminTransaction,
		queue_id: QueueId,
		to_create: QueueToCreate,
		column_ids: &[ColumnId],
	) -> Result<Queue> {
		assert_eq!(column_ids.len(), to_create.columns.len(), "column_ids length must match columns length");

		let namespace_id = to_create.namespace;

		Self::store_queue(txn, queue_id, namespace_id, &to_create)?;
		Self::link_queue_to_namespace(txn, namespace_id, queue_id, to_create.name.text())?;
		Self::insert_queue_columns_with_ids(txn, queue_id, to_create, column_ids)?;

		Self::get_queue(&mut Transaction::Admin(&mut *txn), queue_id)
	}

	#[inline]
	fn reject_existing_queue(txn: &mut AdminTransaction, namespace_id: NamespaceId, name: &Fragment) -> Result<()> {
		let Some(queue) = CatalogStore::find_queue_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			name.text(),
		)?
		else {
			return Ok(());
		};
		let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
		Err(CatalogError::AlreadyExists {
			kind: CatalogObjectKind::Queue,
			namespace: namespace.name().to_string(),
			name: queue.name,
			fragment: name.clone(),
		}
		.into())
	}

	fn store_queue(
		txn: &mut AdminTransaction,
		queue_id: QueueId,
		namespace: NamespaceId,
		to_create: &QueueToCreate,
	) -> Result<()> {
		let mut row = queue::SHAPE.allocate();
		queue::SHAPE.set::<u64>(&mut row, queue::ID, u64::from(queue_id));
		queue::SHAPE.set::<u64>(&mut row, queue::NAMESPACE, u64::from(namespace));
		queue::SHAPE.set_utf8(&mut row, queue::NAME, to_create.name.text());
		encode_dispatch(&mut row, &to_create.dispatch);
		if let Some(done) = to_create.retention.done {
			queue::SHAPE.set::<Duration>(&mut row, queue::RETENTION_DONE, done);
		}
		queue::SHAPE.set::<u32>(&mut row, queue::RETRY_ATTEMPTS, to_create.retry.attempts);
		queue::SHAPE.set::<Duration>(&mut row, queue::RETRY_BACKOFF, to_create.retry.backoff);
		queue::SHAPE.set::<u8>(
			&mut row,
			queue::UNDERLYING,
			if to_create.underlying {
				1
			} else {
				0
			},
		);
		encode_deduplicate(&mut row, to_create.deduplicate.as_ref());

		write_time_source(&queue::SHAPE, &mut row, queue::TIME_DOMAIN, queue::TS, &to_create.time);

		txn.set(&QueueKey::encoded(queue_id), row.freeze())?;

		Ok(())
	}

	fn link_queue_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		queue_id: QueueId,
		name: &str,
	) -> Result<()> {
		let mut row = queue_namespace::SHAPE.allocate();
		queue_namespace::SHAPE.set::<u64>(&mut row, queue_namespace::ID, u64::from(queue_id));
		queue_namespace::SHAPE.set_utf8(&mut row, queue_namespace::NAME, name);

		txn.set(&NamespaceQueueKey::encoded(namespace, queue_id), row.freeze())?;

		Ok(())
	}

	fn insert_queue_columns(txn: &mut AdminTransaction, queue_id: QueueId, to_create: QueueToCreate) -> Result<()> {
		for (idx, col) in to_create.columns.into_iter().enumerate() {
			CatalogStore::create_column(
				txn,
				queue_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					object_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint,
					properties: col.properties,
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}

	fn insert_queue_columns_with_ids(
		txn: &mut AdminTransaction,
		queue_id: QueueId,
		to_create: QueueToCreate,
		column_ids: &[ColumnId],
	) -> Result<()> {
		for (idx, (col, column_id)) in to_create.columns.into_iter().zip(column_ids.iter()).enumerate() {
			CatalogStore::create_column_with_id(
				txn,
				*column_id,
				queue_id,
				ColumnToCreate {
					fragment: Some(col.fragment.clone()),
					namespace_name: String::new(),
					object_name: String::new(),
					column: col.name.text().to_string(),
					constraint: col.constraint,
					properties: col.properties,
					index: ColumnIndex(idx as u8),
					auto_increment: col.auto_increment,
					dictionary_id: col.dictionary_id,
				},
			)?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			id::{ColumnId, NamespaceId, QueueId},
			queue::{Queue, QueueDispatch, QueueRetention, QueueRetry},
		},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, duration::Duration, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::queue::create::{QueueColumnToCreate, QueueToCreate},
		test_utils::ensure_test_namespace,
	};

	fn queue_to_create(name: &str) -> QueueToCreate {
		QueueToCreate {
			name: Fragment::internal(name),
			namespace: NamespaceId(0),
			columns: vec![QueueColumnToCreate {
				name: Fragment::internal("payload"),
				fragment: Fragment::None,
				constraint: TypeConstraint::unconstrained(ValueType::Utf8),
				properties: vec![],
				auto_increment: false,
				dictionary_id: None,
			}],
			dispatch: QueueDispatch::Fifo {
				partitions: 32,
				ordered_by: Some("payload".to_string()),
			},
			retention: QueueRetention {
				done: Some(Duration::from_seconds_const(604800)),
			},
			retry: QueueRetry {
				attempts: 9,
				backoff: Duration::from_seconds_const(30),
			},
			underlying: false,
			deduplicate: None,
			time: TimeSource::Processing,
		}
	}

	#[test]
	fn test_create_queue_round_trips_every_option() {
		// A queue that loses its partition count or retry budget in the def-row codec
		// silently changes behaviour.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let mut to_create = queue_to_create("jobs");
		to_create.namespace = namespace.id();

		let created = CatalogStore::create_queue(&mut txn, to_create).unwrap();

		assert_eq!(created.name, "jobs");
		assert_eq!(created.namespace, namespace.id());
		assert_eq!(created.partitions(), 32);
		assert_eq!(created.ordered_by(), Some("payload"));
		assert_eq!(created.retention.done, Some(Duration::from_seconds_const(604800)));
		assert_eq!(created.retry.attempts, 9);
		assert_eq!(created.retry.backoff, Duration::from_seconds_const(30));
		assert!(!created.underlying);
		assert_eq!(created.columns.len(), 1);
		assert_eq!(created.columns[0].name, "payload");
	}

	#[test]
	fn test_create_queue_without_optional_options() {
		// An absent ordered_by and an absent retention must read back as none, not as an
		// empty-string column name or a zero duration.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			QueueToCreate {
				name: Fragment::internal("plain"),
				namespace: namespace.id(),
				columns: vec![],
				dispatch: QueueDispatch::Fifo {
					partitions: Queue::DEFAULT_PARTITIONS,
					ordered_by: None,
				},
				retention: QueueRetention::default(),
				retry: QueueRetry::default(),
				underlying: false,
				deduplicate: None,
				time: TimeSource::Processing,
			},
		)
		.unwrap();

		assert_eq!(created.partitions(), Queue::DEFAULT_PARTITIONS);
		assert_eq!(created.ordered_by(), None);
		assert_eq!(created.retention.done, None);
		assert_eq!(created.retry.attempts, Queue::DEFAULT_RETRY_ATTEMPTS);
		assert_eq!(created.retry.backoff, Queue::DEFAULT_RETRY_BACKOFF);
	}

	#[test]
	fn test_create_queue_duplicate_name_rejected() {
		// Two queues of one name in a namespace would make name resolution ambiguous.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let mut first = queue_to_create("jobs");
		first.namespace = namespace.id();
		CatalogStore::create_queue(&mut txn, first).unwrap();

		let mut second = queue_to_create("jobs");
		second.namespace = namespace.id();
		let err = CatalogStore::create_queue(&mut txn, second).unwrap_err();

		assert!(err.message.contains("queue"), "error should name the object kind, got: {}", err.message);
		assert!(err.message.contains("jobs"), "error should name the queue, got: {}", err.message);
	}

	#[test]
	fn test_create_queue_with_id_uses_the_supplied_ids() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let mut to_create = queue_to_create("replayed");
		to_create.namespace = namespace.id();

		let created = CatalogStore::create_queue_with_id(&mut txn, QueueId(4242), to_create, &[ColumnId(9001)])
			.unwrap();

		assert_eq!(created.id, QueueId(4242));
		assert_eq!(created.columns[0].id, ColumnId(9001));

		let found =
			CatalogStore::find_queue_by_name(&mut Transaction::Admin(&mut txn), namespace.id(), "replayed")
				.unwrap()
				.unwrap();
		assert_eq!(found.id, created.id);
	}
}

#[cfg(test)]
mod time_declaration_tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::queue::{Queue, QueueDispatch, QueueRetention, QueueRetry},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::fragment::Fragment;

	use super::*;
	use crate::{CatalogStore, test_utils::ensure_test_namespace};

	fn to_create(namespace: NamespaceId, name: &str, time: TimeSource) -> QueueToCreate {
		QueueToCreate {
			name: Fragment::internal(name),
			namespace,
			columns: vec![],
			dispatch: QueueDispatch::Fifo {
				partitions: Queue::DEFAULT_PARTITIONS,
				ordered_by: None,
			},
			deduplicate: None,
			retention: QueueRetention::default(),
			retry: QueueRetry::default(),
			underlying: false,
			time,
		}
	}

	#[test]
	fn a_queue_round_trips_its_populator() {
		// The queue shape is the widest of the source objects and its ts field sits last, so
		// an off-by-one field index surfaces here first.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			to_create(
				namespace.id(),
				"jobs",
				TimeSource::Event {
					ts: "enqueued_at".to_string(),
				},
			),
		)
		.unwrap();

		assert_eq!(created.time.ts(), Some("enqueued_at"));

		let loaded = CatalogStore::find_queue(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("queue must be findable after creation");
		assert_eq!(
			loaded.time,
			TimeSource::Event {
				ts: "enqueued_at".to_string()
			}
		);
		assert_eq!(loaded.retry, QueueRetry::default(), "the neighbouring fields must be undisturbed");
	}

	#[test]
	fn a_bare_queue_round_trips_as_processing() {
		// An undeclared populator must stay absent through the round trip.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			to_create(namespace.id(), "plain_jobs", TimeSource::Processing),
		)
		.unwrap();

		assert_eq!(created.time, TimeSource::Processing);
		assert_eq!(created.time.ts(), None);
	}
}
