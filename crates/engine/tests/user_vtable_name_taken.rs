// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::{interface::catalog::id::NamespaceId, value::batch::empty_batch};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::value_type::ValueType};

#[derive(Clone)]
struct PlainTable;

impl UserVTable for PlainTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		vec![UserVTableColumn::new("id", ValueType::Int4)]
	}

	fn get(&self) -> RecordBatch {
		empty_batch()
	}
}

fn prepared() -> (TestEngine, NamespaceId) {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4 } WITH { time: processing }");
	let namespace = {
		let catalog = t.inner().catalog();
		let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
		let mut txn = Transaction::Query(&mut query_txn);
		catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap().id()
	};
	(t, namespace)
}

#[test]
fn registering_a_user_virtual_table_under_a_taken_name_is_refused() {
	// Without the check at registration a virtual table shadows the table of the same name.
	let (t, namespace) = prepared();
	t.admin("CREATE TABLE test::x { id: int4 }");
	let error = t.inner().register_virtual_table(namespace, "x", PlainTable).expect_err("x is taken");
	assert_eq!(error.code, "CA_137", "{}", error.message);
	assert!(error.message.ends_with("is already taken by a table"), "{}", error.message);
	t.query("from test::x");
}

#[test]
fn a_create_under_a_user_virtual_table_name_is_refused() {
	// Without the virtual table probe a create takes a name a registered virtual table holds.
	let (t, namespace) = prepared();
	t.inner().register_virtual_table(namespace, "v", PlainTable).unwrap();
	for rql in [
		"CREATE TABLE test::v { id: int4 }",
		"CREATE DEFERRED VIEW test::v { id: int4 } AS { FROM test::src }",
		"CREATE NAMESPACE test::v",
	] {
		let result = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
		let error = result.error.unwrap_or_else(|| panic!("must be refused: {rql}"));
		assert_eq!(error.code, "CA_137", "{rql}: {}", error.message);
		assert!(error.message.ends_with("is already taken by a virtual table"), "{rql}: {}", error.message);
	}
}
