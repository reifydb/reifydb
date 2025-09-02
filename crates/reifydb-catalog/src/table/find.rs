// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	EncodableKey, QueryTransaction, SchemaId, SchemaTableKey, TableDef,
	TableId, TableKey, Versioned,
};

use crate::{
	CatalogStore,
	table::layout::{table, table_schema},
};

impl CatalogStore {
	pub fn find_table(
		rx: &mut impl QueryTransaction,
		table: TableId,
	) -> crate::Result<Option<TableDef>> {
		let Some(versioned) = rx.get(&TableKey {
			table,
		}
		.encode())?
		else {
			return Ok(None);
		};

		let row = versioned.row;
		let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
		let schema =
			SchemaId(table::LAYOUT.get_u64(&row, table::SCHEMA));
		let name =
			table::LAYOUT.get_utf8(&row, table::NAME).to_string();

		Ok(Some(TableDef {
			id,
			name,
			schema,
			columns: Self::list_columns(rx, id)?,
			primary_key: Self::find_primary_key(rx, id)?,
		}))
	}

	pub fn find_table_by_name(
		rx: &mut impl QueryTransaction,
		schema: SchemaId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();
		let Some(table) = rx
			.range(SchemaTableKey::full_scan(schema))?
			.find_map(|versioned: Versioned| {
				let row = &versioned.row;
				let table_name = table_schema::LAYOUT
					.get_utf8(row, table_schema::NAME);
				if name == table_name {
					Some(TableId(table_schema::LAYOUT
						.get_u64(
							row,
							table_schema::ID,
						)))
				} else {
					None
				}
			})
		else {
			return Ok(None);
		};

		Ok(Some(Self::get_table(rx, table)?))
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

		let result = CatalogStore::find_table_by_name(
			&mut txn,
			SchemaId(1027),
			"table_two",
		)
		.unwrap()
		.unwrap();
		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.schema, SchemaId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_table_by_name(
			&mut txn,
			SchemaId(1025),
			"some_table",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_table() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_table(&mut txn, "schema_one", "table_one", &[]);
		create_table(&mut txn, "schema_two", "table_two", &[]);
		create_table(&mut txn, "schema_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(
			&mut txn,
			SchemaId(1025),
			"table_four_two",
		)
		.unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_not_found_different_schema() {
		let mut txn = create_test_command_transaction();
		ensure_test_schema(&mut txn);
		create_schema(&mut txn, "schema_one");
		create_schema(&mut txn, "schema_two");
		create_schema(&mut txn, "schema_three");

		create_table(&mut txn, "schema_one", "table_one", &[]);
		create_table(&mut txn, "schema_two", "table_two", &[]);
		create_table(&mut txn, "schema_three", "table_three", &[]);

		let result = CatalogStore::find_table_by_name(
			&mut txn,
			SchemaId(2),
			"table_two",
		)
		.unwrap();
		assert!(result.is_none());
	}
}
