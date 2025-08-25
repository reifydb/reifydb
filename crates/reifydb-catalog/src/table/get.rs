// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{
		EncodableKey, QueryTransaction, SchemaId, TableDef, TableId,
		TableKey,
	},
	internal_error,
};

use crate::{CatalogStore, table::layout::table};

impl CatalogStore {
	pub fn get_table(
		rx: &mut impl QueryTransaction,
		table: TableId,
	) -> crate::Result<TableDef> {
		let versioned = rx
			.get(&TableKey { table }.encode())?
			.ok_or_else(|| {
				Error(internal_error!(
						"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
						table
					))
			})?;

		let row = versioned.row;
		let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
		let schema =
			SchemaId(table::LAYOUT.get_u64(&row, table::SCHEMA));
		let name =
			table::LAYOUT.get_utf8(&row, table::NAME).to_string();

		Ok(TableDef {
			id,
			name,
			schema,
			columns: Self::list_table_columns(rx, id)?,
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{SchemaId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_schema, create_table, ensure_test_schema},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_table(&mut txn, "schema_one", "table_one", &[]);
		create_table(&mut txn, "schema_two", "table_two", &[]);
		create_table(&mut txn, "schema_three", "table_three", &[]);

		let result = CatalogStore::get_table(&mut txn, TableId(1026))
			.unwrap();

		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.schema, SchemaId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_table(&mut txn, "schema_one", "table_one", &[]);
		create_table(&mut txn, "schema_two", "table_two", &[]);
		create_table(&mut txn, "schema_three", "table_three", &[]);

		let err = CatalogStore::get_table(&mut txn, TableId(42))
			.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("TableId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
