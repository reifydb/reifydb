// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Type,
	interface::{
		ColumnKey, EncodableKey, TableColumnKey, TableId,
		VersionedQueryTransaction,
	},
};

use crate::{
	Catalog,
	column::{
        ColumnDef, ColumnId, ColumnIndex,
        layout::{column, table_column},
	},
};

impl Catalog {
	pub fn get_column(
		rx: &mut impl VersionedQueryTransaction,
		column: ColumnId,
	) -> crate::Result<Option<ColumnDef>> {
		match rx.get(&ColumnKey {
			column,
		}
		.encode())?
		{
			None => Ok(None),
			Some(versioned) => {
				let row = versioned.row;

				let id = ColumnId(
					column::LAYOUT
						.get_u64(&row, column::ID),
				);
				let name = column::LAYOUT
					.get_utf8(&row, column::NAME)
					.to_string();
				let value = Type::from_u8(
					column::LAYOUT
						.get_u8(&row, column::VALUE),
				);
				let index = ColumnIndex(
					column::LAYOUT
						.get_u16(&row, column::INDEX),
				);
				let auto_increment = column::LAYOUT
					.get_bool(&row, column::AUTO_INCREMENT);

				let policies =
					Catalog::list_column_policies(rx, id)?;

				Ok(Some(ColumnDef {
					id,
					name,
					ty: value,
					index,
					policies,
					auto_increment,
				}))
			}
		}
	}

	pub fn get_column_by_name(
		rx: &mut impl VersionedQueryTransaction,
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
			Catalog::get_column(rx, id)
		} else {
			Ok(None)
		}
	}
}

#[cfg(test)]
mod tests {
	mod get_column {
		use reifydb_core::Type;
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{
			Catalog, column::ColumnId,
			test_utils::create_test_table_column,
		};

		#[test]
		fn test_ok() {
			let mut txn = create_test_command_transaction();
			create_test_table_column(
				&mut txn,
				"col_1",
				Type::Int1,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_2",
				Type::Int2,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_3",
				Type::Int4,
				vec![],
			);

			let result = Catalog::get_column(&mut txn, ColumnId(2))
				.unwrap()
				.unwrap();

			assert_eq!(result.id, 2);
			assert_eq!(result.name, "col_2");
			assert_eq!(result.ty, Type::Int2);
			assert_eq!(result.auto_increment, false);
		}

		#[test]
		fn test_not_found() {
			let mut txn = create_test_command_transaction();
			create_test_table_column(
				&mut txn,
				"col_1",
				Type::Int1,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_2",
				Type::Int2,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_3",
				Type::Int4,
				vec![],
			);

			let result = Catalog::get_column(&mut txn, ColumnId(4))
				.unwrap();
			assert!(result.is_none());
		}
	}

	mod get_column_by_name {
		use reifydb_core::{Type, interface::TableId};
		use reifydb_transaction::test_utils::create_test_command_transaction;

		use crate::{Catalog, test_utils::create_test_table_column};

		#[test]
		fn test_ok() {
			let mut txn = create_test_command_transaction();
			create_test_table_column(
				&mut txn,
				"col_1",
				Type::Int1,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_2",
				Type::Int2,
				vec![],
			);
			create_test_table_column(
				&mut txn,
				"col_3",
				Type::Int4,
				vec![],
			);

			let result = Catalog::get_column_by_name(
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
			create_test_table_column(
				&mut txn,
				"col_1",
				Type::Int1,
				vec![],
			);

			let result = Catalog::get_column_by_name(
				&mut txn,
				TableId(1),
				"not_found",
			)
			.unwrap();

			assert!(result.is_none());
		}
	}
}
