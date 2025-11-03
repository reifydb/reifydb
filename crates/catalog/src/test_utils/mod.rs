// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	ColumnPolicyKind, CommandTransaction, NamespaceDef, RingBufferDef, RingBufferId, TableDef, TableId, ViewDef,
};
use reifydb_type::TypeConstraint;

use crate::{
	CatalogStore,
	store::{
		column::{ColumnIndex, ColumnToCreate},
		namespace::NamespaceToCreate,
		ring_buffer::create::{RingBufferColumnToCreate, RingBufferToCreate},
		table::TableToCreate,
		view::ViewToCreate,
	},
};

pub fn create_namespace(txn: &mut impl CommandTransaction, namespace: &str) -> NamespaceDef {
	CatalogStore::create_namespace(
		txn,
		NamespaceToCreate {
			namespace_fragment: None,
			name: namespace.to_string(),
		},
	)
	.unwrap()
}

pub fn ensure_test_namespace(txn: &mut impl CommandTransaction) -> NamespaceDef {
	if let Some(result) = CatalogStore::find_namespace_by_name(txn, "test_namespace").unwrap() {
		return result;
	}
	create_namespace(txn, "test_namespace")
}

pub fn ensure_test_table(txn: &mut impl CommandTransaction) -> TableDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_table_by_name(txn, namespace.id, "test_table").unwrap() {
		return result;
	}
	create_table(txn, "test_namespace", "test_table", &[])
}

pub fn create_table(
	txn: &mut impl CommandTransaction,
	namespace: &str,
	table: &str,
	columns: &[crate::store::table::TableColumnToCreate],
) -> TableDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_table(
		txn,
		TableToCreate {
			fragment: None,
			table: table.to_string(),
			namespace: namespace_def.id,
			columns: columns.to_vec(),
			retention_policy: None,
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
	columns: &[crate::store::view::ViewColumnToCreate],
) -> ViewDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

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

pub fn ensure_test_ring_buffer(txn: &mut impl CommandTransaction) -> RingBufferDef {
	let namespace = ensure_test_namespace(txn);

	if let Some(result) = CatalogStore::find_ring_buffer_by_name(txn, namespace.id, "test_ring_buffer").unwrap() {
		return result;
	}
	create_ring_buffer(txn, "test_namespace", "test_ring_buffer", 100, &[])
}

pub fn create_ring_buffer(
	txn: &mut impl CommandTransaction,
	namespace: &str,
	ring_buffer: &str,
	capacity: u64,
	columns: &[RingBufferColumnToCreate],
) -> RingBufferDef {
	// First look up the namespace to get its ID
	let namespace_def = CatalogStore::find_namespace_by_name(txn, namespace).unwrap().expect("Namespace not found");

	CatalogStore::create_ring_buffer(
		txn,
		RingBufferToCreate {
			fragment: None,
			ring_buffer: ring_buffer.to_string(),
			namespace: namespace_def.id,
			capacity,
			columns: columns.to_vec(),
		},
	)
	.unwrap()
}

pub fn create_test_ring_buffer_column(
	txn: &mut impl CommandTransaction,
	ring_buffer_id: RingBufferId,
	name: &str,
	constraint: TypeConstraint,
	policies: Vec<ColumnPolicyKind>,
) {
	let columns = CatalogStore::list_columns(txn, ring_buffer_id).unwrap();

	CatalogStore::create_column(
		txn,
		ring_buffer_id,
		ColumnToCreate {
			fragment: None,
			namespace_name: "test_namespace",
			table: TableId(0), /* Not used - source is passed
			                    * separately */
			table_name: "test_ring_buffer",
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
