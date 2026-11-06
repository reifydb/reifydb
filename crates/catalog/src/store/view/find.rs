// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, QueueId, ViewId},
		series::SeriesKey,
		view::{QueueView, RingBufferView, SeriesView, TableView, View, ViewKind, ViewStorageKind},
	},
	key::{
		catalog::{ViewKey, ViewQueueKey, ViewRingBufferKey},
		namespace::NamespaceViewKey,
	},
	return_internal_error,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::view::shape::{parse_view_sort, view, view_namespace, view_queue, view_ringbuffer},
};

impl CatalogStore {
	pub(crate) fn find_view(rx: &mut Transaction<'_>, id: ViewId) -> Result<Option<View>> {
		let Some(multi) = rx.get(&[ViewKey::new(id).into()])?.pop().flatten() else {
			return Ok(None);
		};

		let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
		let columns = Self::list_columns(rx, id)?;
		let primary_key = Self::find_view_primary_key(rx, id)?;
		let view = decode_view(rx, &bytes, columns, primary_key)?;

		Ok(Some(view))
	}

	pub(crate) fn find_view_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<View>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceViewKey::full_scan(namespace), RangeScope::All, 1024)?;

		let mut found_view = None;
		for entry in stream.by_ref() {
			let multi = entry?;
			let bytes = EncodedCatalogRow::view(&multi.bytes);
			let view_name = view_namespace::get_name(bytes);
			if name == view_name {
				found_view = Some(ViewId(view_namespace::get_id(bytes)));
				break;
			}
		}

		drop(stream);

		let Some(view) = found_view else {
			return Ok(None);
		};

		Ok(Some(Self::get_view(rx, view)?))
	}
}

use reifydb_core::interface::catalog::{column::Column, key::PrimaryKey};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	error::{Diagnostic, Error},
	fragment::Fragment,
};

pub(crate) fn decode_view(
	rx: &mut Transaction<'_>,
	bytes: &EncodedCatalogRow,
	columns: Vec<Column>,
	primary_key: Option<PrimaryKey>,
) -> Result<View> {
	let id = ViewId(view::get_id(bytes));
	let namespace = NamespaceId(view::get_namespace(bytes));
	let name = view::get_name(bytes).to_string();

	let kind_raw = view::get_kind(bytes);
	let kind = match kind_raw {
		0 => ViewKind::Deferred,
		1 => ViewKind::Transactional,
		_ => {
			return Err(Error(Box::new(Diagnostic {
				code: "CA_026".to_string(),
				rql: None,
				message: format!("unknown view kind: {}", kind_raw),
				fragment: Fragment::None,
				label: Some("invalid view kind value".to_string()),
				help: Some("expected 0 (deferred) or 1 (transactional)".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			})));
		}
	};

	let storage_kind = view::get_storage_kind(bytes);
	let sort = parse_view_sort(view::get_sort(bytes));
	let partition_by_encoded = view::get_partition_by(bytes);
	let partition_by: Vec<String> = if partition_by_encoded.is_empty() {
		Vec::new()
	} else {
		partition_by_encoded.split(',').map(|s| s.to_string()).collect()
	};

	Ok(match storage_kind {
		x if x == ViewStorageKind::Table as u8 => View::Table(TableView {
			id,
			name,
			namespace,
			kind,
			columns,
			primary_key,
			partition_by,
			sort,
		}),
		x if x == ViewStorageKind::RingBuffer as u8 => {
			let Some(side) = rx.get(&[ViewRingBufferKey::new(id).into()])?.pop().flatten() else {
				return_internal_error!(
					"view {:?} is a ring buffer view but has no ring buffer row. This indicates a corrupt catalog.",
					id
				)
			};
			let capacity = view_ringbuffer::get_capacity(EncodedCatalogRow::view(&side.bytes));
			View::RingBuffer(RingBufferView {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				partition_by,
				capacity,
				sort,
			})
		}
		x if x == ViewStorageKind::Series as u8 => {
			let key_column = view::get_key_column(bytes).to_string();
			let key_kind_raw = view::get_key_kind(bytes);
			let precision_raw = view::get_precision(bytes);
			let key = SeriesKey::decode(key_kind_raw, precision_raw, key_column);
			let tag_raw = view::get_tag_id(bytes);
			let tag = if tag_raw == 0 {
				None
			} else {
				Some(SumTypeId(tag_raw))
			};
			View::Series(SeriesView {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				partition_by,
				key,
				tag,
				sort,
			})
		}
		x if x == ViewStorageKind::Queue as u8 => {
			let Some(side) = rx.get(&[ViewQueueKey::new(id).into()])?.pop().flatten() else {
				return_internal_error!(
					"view {:?} is a queue view but has no queue row. This indicates a corrupt catalog.",
					id
				)
			};
			View::Queue(QueueView {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				partition_by,
				queue: QueueId(view_queue::get_queue(EncodedCatalogRow::view(&side.bytes))),
				sort,
			})
		}

		_ => View::Table(TableView {
			id,
			name,
			namespace,
			kind,
			columns,
			primary_key,
			partition_by,
			sort,
		}),
	})
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, ViewId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16387),
			"view_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id(), ViewId(16386));
		assert_eq!(result.namespace(), NamespaceId(16387));
		assert_eq!(result.name(), "view_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16385),
			"some_view",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_view() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(16385),
			"view_four_two",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_namespace() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_view(&mut txn, "namespace_one", "view_one", &[]);
		create_view(&mut txn, "namespace_two", "view_two", &[]);
		create_view(&mut txn, "namespace_three", "view_three", &[]);

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId::DEFAULT,
			"view_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}

#[cfg(test)]
mod queue_view_tests {
	use reifydb_core::{
		interface::catalog::{id::QueueId, series::SeriesKey, storage::StorageId, view::View},
		key::catalog::{ViewQueueKey, ViewRingBufferKey},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewStorage, ViewToCreate},
		test_utils::ensure_test_namespace,
	};

	fn create(txn: &mut AdminTransaction, name: &str, storage: ViewStorage) -> View {
		let namespace = ensure_test_namespace(txn);
		CatalogStore::create_deferred_view(
			txn,
			ViewToCreate {
				name: Fragment::internal(name),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("k"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Int8),
					dictionary_id: None,
				}],
				storage,
				sort: vec![],
			},
		)
		.unwrap()
	}

	fn has_side_row(txn: &mut AdminTransaction, view: &View) -> (bool, bool) {
		let ring = txn.get(&[ViewRingBufferKey::new(view.id()).into()]).unwrap().pop().flatten().is_some();
		let queue = txn.get(&[ViewQueueKey::new(view.id()).into()]).unwrap().pop().flatten().is_some();
		(ring, queue)
	}

	#[test]
	fn a_ring_buffer_view_keeps_its_capacity_through_the_store() {
		// A lost capacity reads back as zero and the view would evict every row it is handed.
		let mut txn = create_test_admin_transaction();
		let created = create(
			&mut txn,
			"r",
			ViewStorage::RingBuffer {
				capacity: 777,
				partition_by: vec![],
			},
		);
		let found = CatalogStore::find_view(&mut Transaction::Admin(&mut txn), created.id()).unwrap().unwrap();
		let View::RingBuffer(view) = &found else {
			panic!("expected a ring buffer view, got {found:?}");
		};
		assert_eq!(view.capacity, 777);
		assert_eq!(has_side_row(&mut txn, &found), (true, false));
	}

	#[test]
	fn table_and_series_views_write_no_side_row() {
		// Only ring and queue views carry a side row; any other kind writing one pays for storage it never
		// reads.
		let mut txn = create_test_admin_transaction();
		let table = create(
			&mut txn,
			"t",
			ViewStorage::Table {
				partition_by: vec![],
			},
		);
		let series = create(
			&mut txn,
			"s",
			ViewStorage::Series {
				key: SeriesKey::Integer {
					column: "k".to_string(),
				},
				tag: None,
				partition_by: vec![],
			},
		);
		assert_eq!(has_side_row(&mut txn, &table), (false, false));
		assert_eq!(has_side_row(&mut txn, &series), (false, false));
	}

	#[test]
	fn a_queue_view_without_its_queue_row_is_a_corrupt_catalog() {
		// A default queue id here would point the view at queue 0 and drain someone else's items.
		let mut txn = create_test_admin_transaction();
		let created = create(
			&mut txn,
			"q",
			ViewStorage::Queue {
				queue: QueueId(4321),
				partition_by: vec![],
			},
		);
		txn.remove(vec![(ViewQueueKey::new(created.id()).into(), None)]).unwrap();
		let error = CatalogStore::find_view(&mut Transaction::Admin(&mut txn), created.id()).unwrap_err();
		assert_eq!(error.0.code, "INTERNAL_ERROR");
	}

	#[test]
	fn test_a_queue_view_row_keeps_its_queue_link_and_partition_through_the_store() {
		// Losing the link turns the view into a scan of an empty storage while items pile up unseen.
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);
		let created = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("q"),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("k"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					dictionary_id: None,
				}],
				storage: ViewStorage::Queue {
					queue: QueueId(4321),
					partition_by: vec!["k".to_string()],
				},
				sort: vec![],
			},
		)
		.unwrap();

		let found = CatalogStore::find_view(&mut Transaction::Admin(&mut txn), created.id()).unwrap().unwrap();
		let View::Queue(view) = &found else {
			panic!("expected a queue view, got {found:?}");
		};
		assert_eq!(view.queue, QueueId(4321));
		assert_eq!(view.partition_by, vec!["k".to_string()]);
		assert_eq!(found.storage_id(), StorageId::Queue(QueueId(4321)));
	}
}
