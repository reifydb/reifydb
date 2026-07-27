// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, QueueId},
		queue::{Queue, QueueRetention, QueueRetry},
	},
	key::{EncodableKey, kind::KeyKind, queue::QueueKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::queue::shape::{decode_deduplicate, decode_dispatch, queue},
};

pub(super) struct QueueApplier;

impl CatalogChangeApplier for QueueApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let mut decoded = decode_queue(row);
		decoded.columns = CatalogStore::list_columns(txn, decoded.id)?;
		catalog.cache.set_queue(decoded.id, txn.version(), Some(decoded));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = QueueKey::decode(key).map(|k| k.queue).ok_or(CatalogChangeError::KeyDecodeFailed {
			kind: KeyKind::Queue,
		})?;
		catalog.cache.set_queue(id, txn.version(), None);
		Ok(())
	}
}

fn decode_queue(row: &EncodedRow) -> Queue {
	let id = QueueId(queue::SHAPE.get_u64(row, queue::ID));
	let namespace = NamespaceId(queue::SHAPE.get_u64(row, queue::NAMESPACE));
	let name = queue::SHAPE.get_utf8(row, queue::NAME).to_string();

	Queue {
		id,
		namespace,
		name,
		columns: vec![],
		dispatch: decode_dispatch(row),
		retention: QueueRetention {
			done: queue::SHAPE.try_get_duration(row, queue::RETENTION_DONE),
		},
		retry: QueueRetry {
			attempts: queue::SHAPE.get_u32(row, queue::RETRY_ATTEMPTS),
			backoff: queue::SHAPE.get_duration(row, queue::RETRY_BACKOFF),
		},
		underlying: queue::SHAPE.get_u8(row, queue::UNDERLYING) != 0,
		deduplicate: decode_deduplicate(row),
		time: crate::store::queue::decode_queue_time(row),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::TimeSource;
	use reifydb_core::interface::catalog::{id::NamespaceId, queue::QueueDispatch};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, duration::Duration, value_type::ValueType},
	};

	use super::*;
	use crate::{
		store::queue::create::{QueueColumnToCreate, QueueToCreate},
		test_utils::ensure_test_namespace,
	};

	fn stored_row(txn: &mut AdminTransaction, to_create: QueueToCreate) -> EncodedRow {
		let created = CatalogStore::create_queue(txn, to_create).unwrap();
		let key = QueueKey::encoded(created.id);
		Transaction::Admin(txn).get(&key).unwrap().unwrap().row
	}

	/// The replica applier decodes the def row independently of the primary's
	/// reader, so a drift between the two decoders would replicate a queue with
	/// the wrong retry budget or a silently dropped retention window. Neither
	/// field is visible through system::queues, so only this test can catch it.
	#[test]
	fn test_applier_decodes_every_field_the_store_wrote() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let row = stored_row(
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
					partitions: 512,
					ordered_by: Some("payload".to_string()),
				},
				retention: QueueRetention {
					done: Some(Duration::from_seconds_const(604800)),
				},
				retry: QueueRetry {
					attempts: 11,
					backoff: Duration::from_seconds_const(45),
				},
				underlying: true,
				deduplicate: None,
				time: TimeSource::Processing,
			},
		);

		let decoded = decode_queue(&row);

		assert_eq!(decoded.namespace, namespace.id());
		assert_eq!(decoded.name, "jobs");
		assert_eq!(decoded.partitions(), 512);
		assert_eq!(decoded.ordered_by(), Some("payload"));
		assert_eq!(decoded.retention.done, Some(Duration::from_seconds_const(604800)));
		assert_eq!(decoded.retry.attempts, 11);
		assert_eq!(decoded.retry.backoff, Duration::from_seconds_const(45));
		assert!(decoded.underlying);
	}

	/// The two none states use different encodings - ordered_by an empty string,
	/// retention.done a validity bit - so both must decode back to None rather
	/// than to an empty column name or a zero duration.
	#[test]
	fn test_applier_decodes_the_absent_options_as_none() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let row = stored_row(
			&mut txn,
			QueueToCreate {
				name: Fragment::internal("plain"),
				namespace: namespace.id(),
				columns: vec![],
				dispatch: QueueDispatch::Fifo {
					partitions: 16,
					ordered_by: None,
				},
				retention: QueueRetention::default(),
				retry: QueueRetry::default(),
				underlying: false,
				deduplicate: None,
				time: TimeSource::Processing,
			},
		);

		let decoded = decode_queue(&row);

		assert_eq!(decoded.ordered_by(), None);
		assert_eq!(decoded.retention.done, None);
		assert!(!decoded.underlying);
	}

	/// NamespaceId(0) is a legitimate id, so it must survive the decoder rather
	/// than being confused with an unset field.
	#[test]
	fn test_applier_preserves_the_queue_id_from_the_row() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_queue(
			&mut txn,
			QueueToCreate {
				name: Fragment::internal("ids"),
				namespace: namespace.id(),
				columns: vec![],
				dispatch: QueueDispatch::Fifo {
					partitions: 16,
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

		let row = Transaction::Admin(&mut txn).get(&QueueKey::encoded(created.id)).unwrap().unwrap().row;
		let decoded = decode_queue(&row);

		assert_eq!(decoded.id, created.id);
		assert_ne!(decoded.namespace, NamespaceId(0));
	}
}
