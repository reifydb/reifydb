// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::{
		column::Column,
		id::{ColumnId, NamespaceId},
		object::ObjectId,
	},
	key::column::ColumnKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{CatalogStore, Result, store::column::shape::object_column};

pub struct ColumnInfo {
	pub column: Column,
	pub object_id: ObjectId,
	pub is_view: bool,
	pub entity_kind: &'static str,
	pub entity_name: String,
	pub namespace: NamespaceId,
}

impl CatalogStore {
	pub(crate) fn list_columns(rx: &mut Transaction<'_>, object: impl Into<ObjectId>) -> Result<Vec<Column>> {
		let object = object.into();
		let mut result = vec![];

		let mut ids = Vec::new();
		{
			let stream = rx.range(ColumnKey::full_scan(object), RangeScope::All, 1024)?;
			for entry in stream {
				let multi = entry?;
				let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
				ids.push(ColumnId(object_column::get_id(&bytes)));
			}
		}

		for id in ids {
			result.push(Self::get_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}

	pub(crate) fn list_columns_all(rx: &mut Transaction<'_>) -> Result<Vec<ColumnInfo>> {
		let mut result = Vec::new();

		let tables = CatalogStore::list_tables(rx)?;
		for table in tables {
			let columns = CatalogStore::list_columns(rx, table.id)?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					object_id: table.id.into(),
					is_view: false,
					entity_kind: "table",
					entity_name: table.name.clone(),
					namespace: table.namespace,
				});
			}
		}

		let views = CatalogStore::list_views_all(rx)?;
		for view in views {
			let columns = CatalogStore::list_columns(rx, view.id())?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					object_id: view.id().into(),
					is_view: true,
					entity_kind: "view",
					entity_name: view.name().to_string(),
					namespace: view.namespace(),
				});
			}
		}

		let ringbuffers = CatalogStore::list_ringbuffers(rx)?;
		for ringbuffer in ringbuffers {
			let columns = CatalogStore::list_columns(rx, ringbuffer.id)?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					object_id: ringbuffer.id.into(),
					is_view: false,
					entity_kind: "ring buffer",
					entity_name: ringbuffer.name.clone(),
					namespace: ringbuffer.namespace,
				});
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{column::ColumnIndex, id::TableId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use crate::{CatalogStore, store::column::create::ColumnToCreate, test_utils::ensure_test_table};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		// Created out of index order, so listing must sort rather than echo insertion order.
		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "b_col".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Int4),
				properties: vec![],
				index: ColumnIndex(1),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap();

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "a_col".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Boolean),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), TableId(1)).unwrap();
		assert_eq!(columns.len(), 2);

		assert_eq!(columns[0].name, "a_col"); // index 0
		assert_eq!(columns[1].name, "b_col"); // index 1

		assert_eq!(columns[0].index, ColumnIndex(0));
		assert_eq!(columns[1].index, ColumnIndex(1));

		assert_eq!(columns[0].auto_increment, false);
		assert_eq!(columns[1].auto_increment, true);
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), TableId(1)).unwrap();
		assert!(columns.is_empty());
	}

	#[test]
	fn test_table_does_not_exist() {
		let mut txn = create_test_admin_transaction();

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), TableId(1)).unwrap();
		assert!(columns.is_empty());
	}
}
