// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Type,
	interface::{
		ActiveCommandTransaction, ColumnPolicyKind, TableDef, TableId,
		Transaction, ViewDef,
	},
};

use crate::{
	Catalog,
	schema::{SchemaDef, SchemaToCreate},
	table,
	table::TableToCreate,
	table_column::{ColumnIndex, TableColumnToCreate},
	view,
	view::ViewToCreate,
};

pub fn create_schema<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
	schema: &str,
) -> SchemaDef {
	Catalog::create_schema(
		txn,
		SchemaToCreate {
			schema_fragment: None,
			name: schema.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_schema<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
) -> SchemaDef {
	if let Some(result) =
		Catalog::find_schema_by_name(txn, "test_schema").unwrap()
	{
		return result;
	}
	create_schema(txn, "test_schema")
}

pub fn ensure_test_table<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
) -> TableDef {
	let schema = ensure_test_schema(txn);
	if let Some(result) =
		Catalog::find_table_by_name(txn, schema.id, "test_table")
			.unwrap()
	{
		return result;
	}
	create_table(txn, "test_schema", "test_table", &[])
}

pub fn create_table<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
	schema: &str,
	table: &str,
	columns: &[table::TableColumnToCreate],
) -> TableDef {
	Catalog::create_table(
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

pub fn create_test_table_column<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
	name: &str,
	value: Type,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn);

	let columns = Catalog::list_table_columns(txn, TableId(1)).unwrap();

	Catalog::create_table_column(
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

pub fn create_view<T: Transaction>(
	txn: &mut ActiveCommandTransaction<T>,
	schema: &str,
	view: &str,
	columns: &[view::ViewColumnToCreate],
) -> ViewDef {
	Catalog::create_view(
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
