// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ColumnPolicyKind, CommandTransaction, NamespaceDef, TableDef, TableId,
	ViewDef,
};
use reifydb_type::TypeConstraint;

use crate::{
	CatalogStore,
	column::{ColumnIndex, ColumnToCreate},
	namespace::NamespaceToCreate,
	table,
	table::TableToCreate,
	view,
	view::ViewToCreate,
};

pub fn create_namespace(
	txn: &mut impl CommandTransaction,
	namespace: &str,
) -> NamespaceDef {
	CatalogStore::create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: None,
			name: namespace.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_namespace(
	txn: &mut impl CommandTransaction,
) -> NamespaceDef {
	if let Some(result) =
		CatalogStore::find_namespace_by_name(txn, "test_namespace")
			.unwrap()
	{
		return result;
	}
	create_namespace(txn, "test_namespace")
}

pub fn ensure_test_table(txn: &mut impl CommandTransaction) -> TableDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_table_by_name(
		txn,
		namespace.id,
		"test_table",
	)
	.unwrap()
	{
		return result;
	}
	create_table(txn, "test_namespace", "test_table", &[])
}

pub fn create_table(
	txn: &mut impl CommandTransaction,
	namespace: &str,
	table: &str,
	columns: &[table::TableColumnToCreate],
) -> TableDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace)
			.unwrap()
			.expect("Namespace not found");

	CatalogStore::create_table(
		txn,
		TableToCreate {
			fragment: None,
			table: table.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn create_test_column(
	txn: &mut impl CommandTransaction,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	ensure_test_table(txn);

	let columns = CatalogStore::list_columns(txn, TableId(1)).unwrap();

	CatalogStore::create_column(
		txn,
		TableId(1),
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace",
			table: TableId(1025),
			table_name: "test_table",
			column: name.to_string(),
			constraint,
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
	namespace: &str,
	view: &str,
	columns: &[view::ViewColumnToCreate],
) -> ViewDef {
	// First look up the namespace to get its ID
	let namespace_def =
		CatalogStore::find_namespace_by_name(txn, namespace)
			.unwrap()
			.expect("Namespace not found");

	CatalogStore::create_deferred_view(
		txn,
		ViewToCreate {
			fragment: None,
			name: view.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}
