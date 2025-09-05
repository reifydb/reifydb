// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnKey, QueryTransaction, SourceId};

use crate::{
	CatalogStore,
	column::{ColumnDef, ColumnId, layout::table_column},
	transaction::CatalogTransaction,
};

/// Extended column information for system catalogs
pub struct ColumnInfo {
	pub column: ColumnDef,
	pub source_id: SourceId,
	pub is_view: bool,
}

impl CatalogStore {
	pub fn list_columns(
		rx: &mut impl QueryTransaction,
		source: impl Into<SourceId>,
	) -> crate::Result<Vec<ColumnDef>> {
		let source = source.into();
		let mut result = vec![];

		let ids =
			rx.range(ColumnKey::full_scan(source))?
				.map(|versioned| {
					let row = versioned.row;
					ColumnId(table_column::LAYOUT.get_u64(
						&row,
						table_column::ID,
					))
				})
				.collect::<Vec<_>>();

		for id in ids {
			result.push(Self::get_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}

	pub fn list_columns_all(
		rx: &mut (impl QueryTransaction + CatalogTransaction),
	) -> crate::Result<Vec<ColumnInfo>> {
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

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::TableId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore,
		column::{ColumnIndex, ColumnToCreate},
		test_utils::ensure_test_table,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		// Create columns out of order

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: TableId(1),
				table_name: "test_table",
				column: "b_col".to_string(),
				value: Type::Int4,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: true,
			},
		)
		.unwrap();

		CatalogStore::create_column(
			&mut txn,
			TableId(1),
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: TableId(1),
				table_name: "test_table",
				column: "a_col".to_string(),
				value: Type::Boolean,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let columns = CatalogStore::list_columns(&mut txn, TableId(1))
			.unwrap();
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
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		let columns = CatalogStore::list_columns(&mut txn, TableId(1))
			.unwrap();
		assert!(columns.is_empty());
	}

	#[test]
	fn test_table_does_not_exist() {
		let mut txn = create_test_command_transaction();

		let columns = CatalogStore::list_columns(&mut txn, TableId(1))
			.unwrap();
		assert!(columns.is_empty());
	}
}
