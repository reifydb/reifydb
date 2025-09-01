// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ColumnPolicyKind, CommandTransaction, SchemaDef, TableDef, TableId,
	ViewDef,
};
use reifydb_type::Type;

use crate::{
	CatalogStore,
	column::{ColumnIndex, ColumnToCreate},
	schema::SchemaToCreate,
	table,
	table::TableToCreate,
	view,
	view::ViewToCreate,
};

pub fn create_schema(
	txn: &mut impl CommandTransaction,
	schema: &str,
) -> SchemaDef {
	CatalogStore::create_schema(
		txn,
		SchemaToCreate {
			schema_fragment: None,
			name: schema.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_schema(txn: &mut impl CommandTransaction) -> SchemaDef {
	if let Some(result) =
		CatalogStore::find_schema_by_name(txn, "test_schema").unwrap()
	{
		return result;
	}
	create_schema(txn, "test_schema")
}

pub fn ensure_test_table(txn: &mut impl CommandTransaction) -> TableDef {
	let schema = ensure_test_schema(txn);

	if let Some(result) =
		CatalogStore::find_table_by_name(txn, schema.id, "test_table")
			.unwrap()
	{
		return result;
	}
	create_table(txn, "test_schema", "test_table", &[])
}

pub fn create_table(
	txn: &mut impl CommandTransaction,
	schema: &str,
	table: &str,
	columns: &[table::TableColumnToCreate],
) -> TableDef {
	// First look up the schema to get its ID
	let schema_def = CatalogStore::find_schema_by_name(txn, schema)
		.unwrap()
		.expect("Schema not found");

	CatalogStore::create_table(
		txn,
		TableToCreate {
			fragment: None,
			table: table.to_string(),
			schema: schema_def.id,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn create_test_column(
	txn: &mut impl CommandTransaction,
	name: &str,
	value: Type,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn);

	let columns =
		CatalogStore::list_table_columns(txn, TableId(1)).unwrap();

	CatalogStore::create_column(
		txn,
		TableId(1),
		ColumnToCreate {
			fragment: None,
			schema_name: "test_schema",
			table: TableId(1025),
			table_name: "test_table",
			column: name.to_string(),
			value,
			if_not_exists: false,
			policies,
			index: ColumnIndex(columns.len() as u16),
			auto_increment: false,
		},
	)
	.unwrap();
}

pub fn create_view(
	txn: &mut impl CommandTransaction,
	schema: &str,
	view: &str,
	columns: &[view::ViewColumnToCreate],
) -> ViewDef {
	// First look up the schema to get its ID
	let schema_def = CatalogStore::find_schema_by_name(txn, schema)
		.unwrap()
		.expect("Schema not found");

	CatalogStore::create_deferred_view(
		txn,
		ViewToCreate {
			fragment: None,
			name: view.to_string(),
			schema: schema_def.id,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}
