// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use column::LAYOUT;
use reifydb_core::{
	Error, Type,
	interface::{ColumnsKey, EncodableKey, QueryTransaction},
	internal_error,
};

use crate::{
	CatalogStore,
	column::{ColumnDef, ColumnId, ColumnIndex, layout::column},
};

impl CatalogStore {
	pub fn get_column(
		rx: &mut impl QueryTransaction,
		column: ColumnId,
	) -> crate::Result<ColumnDef> {
		let versioned = rx
			.get(&ColumnsKey { column }.encode())?
			.ok_or_else(|| {
				Error(internal_error!(
					"Table column with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
					column
				))
			})?;

		let row = versioned.row;

		let id = ColumnId(LAYOUT.get_u64(&row, column::ID));
		let name = LAYOUT.get_utf8(&row, column::NAME).to_string();
		let value = Type::from_u8(LAYOUT.get_u8(&row, column::VALUE));
		let index = ColumnIndex(LAYOUT.get_u16(&row, column::INDEX));
		let auto_increment =
			LAYOUT.get_bool(&row, column::AUTO_INCREMENT);

		let policies = Self::list_table_column_policies(rx, id)?;

		Ok(ColumnDef {
			id,
			name,
			ty: value,
			index,
			policies,
			auto_increment,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::Type;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore, column::ColumnId, test_utils::create_test_column,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", Type::Int1, vec![]);
		create_test_column(&mut txn, "col_2", Type::Int2, vec![]);
		create_test_column(&mut txn, "col_3", Type::Int4, vec![]);

		let result = CatalogStore::get_column(&mut txn, ColumnId(8194))
			.unwrap();

		assert_eq!(result.id, ColumnId(8194));
		assert_eq!(result.name, "col_2");
		assert_eq!(result.ty, Type::Int2);
		assert_eq!(result.auto_increment, false);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		create_test_column(&mut txn, "col_1", Type::Int1, vec![]);
		create_test_column(&mut txn, "col_2", Type::Int2, vec![]);
		create_test_column(&mut txn, "col_3", Type::Int4, vec![]);

		let err = CatalogStore::get_column(&mut txn, ColumnId(4))
			.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ColumnId(4)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
