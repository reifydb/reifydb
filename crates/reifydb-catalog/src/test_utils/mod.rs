// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	schema::SchemaToCreate,
	table,
	table::TableToCreate,
	table_column::{ColumnIndex, TableColumnToCreate},
	view,
	view::ViewToCreate,
	Catalog,
};
use reifydb_core::interface::CommandTransaction;
use reifydb_core::{
	interface::{
		ColumnPolicyKind, SchemaDef, TableDef, TableId
		, ViewDef,
	},
	Type,
};

pub fn create_schema(
	txn: &mut impl CommandTransaction,
	schema: &str,
) -> SchemaDef {
	let catalog = Catalog::new();
	catalog.create_schema(
		txn,
		SchemaToCreate {
			schema_fragment: None,
			name: schema.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_schema(txn: &mut impl CommandTransaction) -> SchemaDef {
	let catalog = Catalog::new();
	if let Some(result) =
		catalog.find_schema_by_name(txn, "test_schema").unwrap()
	{
		return result;
	}
	create_schema(txn, "test_schema")
}

pub fn ensure_test_table(txn: &mut impl CommandTransaction) -> TableDef {
	let schema = ensure_test_schema(txn);
	let catalog = Catalog::new();
	if let Some(result) = catalog
		.find_table_by_name(txn, schema.id, "test_table")
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
	let catalog = Catalog::new();
	catalog.create_table(
		txn,
		TableToCreate {
			fragment: None,
			schema: schema.to_string(),
			table: table.to_string(),
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn create_test_table_column(
	txn: &mut impl CommandTransaction,
	name: &str,
	value: Type,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn);

	let catalog = Catalog::new();
	let columns = catalog.list_table_columns(txn, TableId(1)).unwrap();

	catalog.create_table_column(
		txn,
		TableId(1),
		TableColumnToCreate {
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
	let catalog = Catalog::new();
	catalog.create_deferred_view(
		txn,
		ViewToCreate {
			fragment: None,
			schema: schema.to_string(),
			view: view.to_string(),
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}
