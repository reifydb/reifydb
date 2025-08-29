// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{ColumnKey, QueryTransaction, StoreId};

use crate::{
	CatalogStore,
	column::{ColumnDef, ColumnId, layout::table_column},
};

impl CatalogStore {
	pub fn list_table_columns(
		rx: &mut impl QueryTransaction,
		store: impl Into<StoreId>,
	) -> crate::Result<Vec<ColumnDef>> {
		let store = store.into();
		let mut result = vec![];

		let ids =
			rx.range(ColumnKey::full_scan(store))?
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
}

#[cfg(test)]
mod tests {
	use reifydb_core::{Type, interface::TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

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
				value: Type::Bool,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		let columns =
			CatalogStore::list_table_columns(&mut txn, TableId(1))
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

		let columns =
			CatalogStore::list_table_columns(&mut txn, TableId(1))
				.unwrap();
		assert!(columns.is_empty());
	}

	#[test]
	fn test_table_does_not_exist() {
		let mut txn = create_test_command_transaction();

		let columns =
			CatalogStore::list_table_columns(&mut txn, TableId(1))
				.unwrap();
		assert!(columns.is_empty());
	}
}
