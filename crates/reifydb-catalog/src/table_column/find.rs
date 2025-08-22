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
	pub fn find_table_column_by_name(
		&self,
		rx: &mut impl UnderlyingQueryTransaction,
		table: TableId,
		column_name: &str,
	) -> crate::Result<Option<ColumnDef>> {
		let maybe_id = rx
			.range(TableColumnKey::full_scan(table))?
			.find_map(|versioned| {
				let row = versioned.row;
				let column =
					ColumnId(table_column::LAYOUT.get_u64(
						&row,
						table_column::ID,
					));
				let name = table_column::LAYOUT
					.get_utf8(&row, table_column::NAME);

				if name == column_name {
					Some(column)
				} else {
					None
				}
			});

		if let Some(id) = maybe_id {
			Ok(Some(self.get_table_column(rx, id)?))
		} else {
			Ok(None)
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{interface::TableId, Type};
	use reifydb_transaction::test_utils::create_test_command_transaction;

	use crate::{test_utils::create_test_table_column, Catalog};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		create_test_table_column(&mut txn, "col_1", Type::Int1, vec![]);
		create_test_table_column(&mut txn, "col_2", Type::Int2, vec![]);
		create_test_table_column(&mut txn, "col_3", Type::Int4, vec![]);

		let catalog = Catalog::new();
		let result = catalog
			.find_table_column_by_name(
				&mut txn,
				TableId(1),
				"col_3",
			)
			.unwrap()
			.unwrap();

		assert_eq!(result.id, 3);
		assert_eq!(result.name, "col_3");
		assert_eq!(result.ty, Type::Int4);
		assert_eq!(result.auto_increment, false);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		create_test_table_column(&mut txn, "col_1", Type::Int1, vec![]);

		let catalog = Catalog::new();
		let result = catalog
			.find_table_column_by_name(
				&mut txn,
				TableId(1),
				"not_found",
			)
			.unwrap();

		assert!(result.is_none());
	}
}
