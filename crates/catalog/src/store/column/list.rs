// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{column::ColumnDef, id::ColumnId, primitive::PrimitiveId},
	key::column::ColumnKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::column::schema::primitive_column};

/// Extended column information for system catalogs
pub struct ColumnInfo {
	pub column: ColumnDef,
	pub source_id: PrimitiveId,
	pub is_view: bool,
}

impl CatalogStore {
	pub(crate) fn list_columns(
		rx: &mut Transaction<'_>,
		source: impl Into<PrimitiveId>,
	) -> crate::Result<Vec<ColumnDef>> {
		let source = source.into();
		let mut result = vec![];

		// Collect column IDs first to avoid holding stream borrow
		let mut ids = Vec::new();
		{
			let mut stream = rx.range(ColumnKey::full_scan(source), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let row = multi.values;
				ids.push(ColumnId(primitive_column::SCHEMA.get_u64(&row, primitive_column::ID)));
			}
		}

		for id in ids {
			result.push(Self::get_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}

	pub(crate) fn list_columns_all(rx: &mut Transaction<'_>) -> crate::Result<Vec<ColumnInfo>> {
		let mut result = Vec::new();

		// Get all tables
		let tables = CatalogStore::list_tables_all(rx)?;
		for table in tables {
			let columns = CatalogStore::list_columns(rx, table.id)?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: table.id.into(),
					is_view: false,
				});
			}
		}

		// Get all views
		let views = CatalogStore::list_views_all(rx)?;
		for view in views {
			let columns = CatalogStore::list_columns(rx, view.id)?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: view.id.into(),
					is_view: true,
				});
			}
		}

		// Get all ring buffers
		let ringbuffers = CatalogStore::list_ringbuffers_all(rx)?;
		for ringbuffer in ringbuffers {
			let columns = CatalogStore::list_columns(rx, ringbuffer.id)?;
			for column in columns {
				result.push(ColumnInfo {
					column,
					source_id: ringbuffer.id.into(),
					is_view: false,
				});
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{column::ColumnIndex, id::TableId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{CatalogStore, store::column::create::ColumnToCreate, test_utils::ensure_test_table};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_table(&mut txn);

		// Create columns out of order
		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "b_col".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Int4),
				policies: vec![],
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
				primitive_name: "test_table".to_string(),
				column: "a_col".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Boolean),
				policies: vec![],
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
