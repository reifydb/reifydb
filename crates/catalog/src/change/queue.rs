// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
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
	store::queue::{
		decode_queue_time,
		shape::{decode_deduplicate, decode_dispatch, queue},
	},
};

pub(super) struct QueueApplier;

impl CatalogChangeApplier for QueueApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, bytes: &EncodedBytes) -> Result<()> {
		txn.set(key, bytes.clone())?;
		let mut decoded = decode_queue(bytes);
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

fn decode_queue(bytes: &EncodedBytes) -> Queue {
	let id = QueueId(queue::get_id(bytes));
	let namespace = NamespaceId(queue::get_namespace(bytes));
	let name = queue::get_name(bytes).to_string();

	Queue {
		id,
		namespace,
		name,
		columns: vec![],
		dispatch: decode_dispatch(bytes),
		retention: QueueRetention {
			done: queue::try_get_retention_done(bytes),
		},
		retry: QueueRetry {
			attempts: queue::get_retry_attempts(bytes),
			backoff: queue::get_retry_backoff(bytes),
		},
		underlying: queue::get_underlying(bytes) != 0,
		deduplicate: decode_deduplicate(bytes),
		time: decode_queue_time(bytes),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{id::NamespaceId, queue::QueueDispatch},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
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

	fn stored_row(txn: &mut AdminTransaction, to_create: QueueToCreate) -> EncodedBytes {
		let created = CatalogStore::create_queue(txn, to_create).unwrap();
		let key = QueueKey::encoded(created.id);
		Transaction::Admin(txn).get(&key).unwrap().unwrap().bytes
	}

	#[test]
	fn test_applier_decodes_every_field_the_store_wrote() {
		// The replica applier decodes the def row independently of the primary's reader, and
		// neither retry nor retention is visible through system::queues, so drift between the
		// two decoders would replicate a wrong retry budget with nothing else to catch it.
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

	#[test]
	fn test_applier_decodes_the_absent_options_as_none() {
		// The two none states use different encodings - ordered_by an empty string,
		// retention.done a validity bit - and both must decode back to none, not to an
		// empty column name or a zero duration.
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

	#[test]
	fn test_applier_preserves_the_queue_id_from_the_row() {
		// NamespaceId(0) is a legitimate id and must not be confused with an unset field.
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

		let bytes = Transaction::Admin(&mut txn).get(&QueueKey::encoded(created.id)).unwrap().unwrap().bytes;
		let decoded = decode_queue(&bytes);

		assert_eq!(decoded.id, created.id);
		assert_ne!(decoded.namespace, NamespaceId(0));
	}
}
