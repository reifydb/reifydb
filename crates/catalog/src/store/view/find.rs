// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{NamespaceId, RingBufferId, SeriesId, TableId, ViewId},
		series::TimestampPrecision,
		view::{RingBufferViewDef, SeriesViewDef, TableViewDef, ViewDef, ViewKind, ViewStorageKind},
	},
	key::{namespace_view::NamespaceViewKey, view::ViewKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::SumTypeId;

use crate::{
	CatalogStore, Result,
	store::view::schema::{view, view_namespace},
};

impl CatalogStore {
	pub(crate) fn find_view(rx: &mut Transaction<'_>, id: ViewId) -> Result<Option<ViewDef>> {
		let Some(multi) = rx.get(&ViewKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let columns = Self::list_columns(rx, id)?;
		let primary_key = Self::find_view_primary_key(rx, id)?;
		let view_def = decode_view_def(&row, columns, primary_key);

		Ok(Some(view_def))
	}

	pub(crate) fn find_view_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<ViewDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceViewKey::full_scan(namespace), 1024)?;

		let mut found_view = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let view_name = view_namespace::SCHEMA.get_utf8(row, view_namespace::NAME);
			if name == view_name {
				found_view = Some(ViewId(view_namespace::SCHEMA.get_u64(row, view_namespace::ID)));
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

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{column::ColumnDef, key::PrimaryKeyDef},
};

pub(crate) fn decode_view_def(
	row: &EncodedValues,
	columns: Vec<ColumnDef>,
	primary_key: Option<PrimaryKeyDef>,
) -> ViewDef {
	let id = ViewId(view::SCHEMA.get_u64(row, view::ID));
	let namespace = NamespaceId(view::SCHEMA.get_u64(row, view::NAMESPACE));
	let name = view::SCHEMA.get_utf8(row, view::NAME).to_string();

	let kind = match view::SCHEMA.get_u8(row, view::KIND) {
		0 => ViewKind::Deferred,
		1 => ViewKind::Transactional,
		_ => unimplemented!(),
	};

	let storage_kind = view::SCHEMA.get_u8(row, view::STORAGE_KIND);
	let underlying_primitive_id = view::SCHEMA.get_u64(row, view::UNDERLYING_PRIMITIVE_ID);

	match storage_kind {
		x if x == ViewStorageKind::Table as u8 => ViewDef::Table(TableViewDef {
			id,
			name,
			namespace,
			kind,
			columns,
			primary_key,
			underlying: TableId(underlying_primitive_id),
		}),
		x if x == ViewStorageKind::RingBuffer as u8 => {
			let capacity = view::SCHEMA.get_u64(row, view::CAPACITY);
			let propagate_evictions = view::SCHEMA.get_u8(row, view::PROPAGATE_EVICTIONS) != 0;
			ViewDef::RingBuffer(RingBufferViewDef {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				underlying: RingBufferId(underlying_primitive_id),
				capacity,
				propagate_evictions,
			})
		}
		x if x == ViewStorageKind::Series as u8 => {
			let ts_col = view::SCHEMA.get_utf8(row, view::TIMESTAMP_COLUMN).to_string();
			let timestamp_column = if ts_col.is_empty() {
				None
			} else {
				Some(ts_col)
			};
			let precision = match view::SCHEMA.get_u8(row, view::PRECISION) {
				0 => TimestampPrecision::Millisecond,
				1 => TimestampPrecision::Microsecond,
				2 => TimestampPrecision::Nanosecond,
				_ => TimestampPrecision::Millisecond,
			};
			let tag_raw = view::SCHEMA.get_u64(row, view::TAG_ID);
			let tag = if tag_raw == 0 {
				None
			} else {
				Some(SumTypeId(tag_raw))
			};
			ViewDef::Series(SeriesViewDef {
				id,
				name,
				namespace,
				kind,
				columns,
				primary_key,
				underlying: SeriesId(underlying_primitive_id),
				timestamp_column,
				precision,
				tag,
			})
		}
		// Default to table for backwards compat during transition (storage_kind=0 from old data)
		_ => ViewDef::Table(TableViewDef {
			id,
			name,
			namespace,
			kind,
			columns,
			primary_key,
			underlying: TableId(underlying_primitive_id),
		}),
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, ViewId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
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
			NamespaceId(1027),
			"view_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id(), ViewId(1026));
		assert_eq!(result.namespace(), NamespaceId(1027));
		assert_eq!(result.name(), "view_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_view_by_name(
			&mut Transaction::Admin(&mut txn),
			NamespaceId(1025),
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
			NamespaceId(1025),
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
