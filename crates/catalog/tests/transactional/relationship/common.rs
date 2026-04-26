// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::id::{ColumnId, NamespaceId, TableId};
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

/// Creates a namespace `<prefix>` with two tables `parent(id)` and `child(parent_id)`,
/// and exposes their ids + the (only) column id of each. Lets relationship tests focus
/// on relationship behaviour without re-doing namespace + table boilerplate.
pub struct SourceFixture {
	pub namespace: NamespaceId,
	pub source_table: TableId,
	pub source_column: ColumnId,
	pub target_table: TableId,
	pub target_column: ColumnId,
}

impl SourceFixture {
	pub fn new(t: &TestEngine, ns: &str) -> Self {
		t.admin(&format!("CREATE NAMESPACE {ns}"));
		t.admin(&format!("CREATE TABLE {ns}::parent {{ id: int4 }}"));
		t.admin(&format!("CREATE TABLE {ns}::child {{ parent_id: int4 }}"));

		let catalog = t.catalog();
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let namespace =
			catalog.find_namespace_by_name(&mut Transaction::Admin(&mut probe), ns).unwrap().unwrap().id();
		let source_table = catalog
			.find_table_by_name(&mut Transaction::Admin(&mut probe), namespace, "parent")
			.unwrap()
			.unwrap()
			.id;
		let target_table = catalog
			.find_table_by_name(&mut Transaction::Admin(&mut probe), namespace, "child")
			.unwrap()
			.unwrap()
			.id;
		let source_columns = catalog.list_columns(&mut Transaction::Admin(&mut probe), source_table).unwrap();
		let source_column = source_columns.iter().find(|c| c.name == "id").unwrap().id;
		let target_columns = catalog.list_columns(&mut Transaction::Admin(&mut probe), target_table).unwrap();
		let target_column = target_columns.iter().find(|c| c.name == "parent_id").unwrap().id;

		Self {
			namespace,
			source_table,
			source_column,
			target_table,
			target_column,
		}
	}
}
