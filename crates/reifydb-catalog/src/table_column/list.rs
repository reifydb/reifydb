// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	TableColumnKey, TableId, UnderlyingQueryTransaction,
	VersionedQueryTransaction,
};

use crate::{
	table_column::{layout::table_column, ColumnDef, ColumnId},
	Catalog,
};

impl Catalog {
	pub fn list_table_columns(
		&self,
		rx: &mut impl UnderlyingQueryTransaction,
		table: TableId,
	) -> crate::Result<Vec<ColumnDef>> {
		let mut result = vec![];

		let ids =
			rx.range(TableColumnKey::full_scan(table))?
				.map(|versioned| {
					let row = versioned.row;
					ColumnId(table_column::LAYOUT.get_u64(
						&row,
						table_column::ID,
					))
				})
				.collect::<Vec<_>>();

		for id in ids {
			result.push(self.get_table_column(rx, id)?);
		}

		result.sort_by_key(|c| c.index);

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{interface::TableId, Type};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::{
		table_column::{ColumnIndex, TableColumnToCreate},
		test_utils::ensure_test_table,
		Catalog,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_table(&mut txn);

		// Create columns out of order
		let catalog = Catalog::new();
		catalog.create_table_column(
			&mut txn,
			TableId(1),
			TableColumnToCreate {
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

		catalog.create_table_column(
			&mut txn,
			TableId(1),
			TableColumnToCreate {
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

		let columns = catalog
			.list_table_columns(&mut txn, TableId(1))
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
		let catalog = Catalog::new();
		let columns = catalog
			.list_table_columns(&mut txn, TableId(1))
			.unwrap();
		assert!(columns.is_empty());
	}

	#[test]
	fn test_table_does_not_exist() {
		let mut txn = create_test_command_transaction();
		let catalog = Catalog::new();
		let columns = catalog
			.list_table_columns(&mut txn, TableId(1))
			.unwrap();
		assert!(columns.is_empty());
	}
}
