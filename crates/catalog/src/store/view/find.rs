// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
		series::SeriesKey,
		view::{RingBufferView, SeriesView, TableView, View, ViewKind, ViewStorageKind},
	},
	key::{namespace_view::NamespaceViewKey, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::view::shape::{parse_view_sort, view, view_namespace},
};

impl CatalogStore {
	pub(crate) fn find_view(rx: &mut Transaction<'_>, id: ViewId) -> Result<Option<View>> {
		let Some(multi) = rx.get(&ViewKey::encoded(id))? else {
			return Ok(None);
		};

		let bytes = multi.bytes;
		let columns = Self::list_columns(rx, id)?;
		let primary_key = Self::find_view_primary_key(rx, id)?;
		let view = decode_view(&bytes, columns, primary_key)?;

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
			let bytes = &multi.bytes;
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

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::interface::catalog::{column::Column, key::PrimaryKey};
use reifydb_transaction::multi::RangeScope;
use reifydb_value::{
	error::{Diagnostic, Error},
	fragment::Fragment,
};

pub(crate) fn decode_view(bytes: &EncodedBytes, columns: Vec<Column>, primary_key: Option<PrimaryKey>) -> Result<View> {
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
	let storage_id = view::get_storage_id(bytes);
	let sort = parse_view_sort(view::get_sort(bytes));

	Ok(match storage_kind {
		x if x == ViewStorageKind::Table as u8 => View::Table(TableView {
			id,
			name,
			namespace,
			kind,
			columns,
			primary_key,
			storage: TableId(storage_id),
			sort,
		}),
		x if x == ViewStorageKind::RingBuffer as u8 => {
			let capacity = view::get_capacity(bytes);
			View::RingBuffer(RingBufferView {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				storage: RingBufferId(storage_id),
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
				storage: SeriesId(storage_id),
				key,
				tag,
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
			storage: TableId(storage_id),
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
