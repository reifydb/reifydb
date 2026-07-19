// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::Value;

#[test]
fn test_ringbuffer_delete_partition_to_zero_removes_partition_metadata() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 2, partition: { by: { region } } }",
	);

	t.command(r#"INSERT test::rb [{ region: "us", n: 1 }, { region: "us", n: 2 }, { region: "eu", n: 100 }]"#);

	t.command(r#"DELETE test::rb FILTER { region == "us" }"#);

	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	let rb = catalog.find_ringbuffer_by_name(&mut txn, namespace.id(), "rb").unwrap().unwrap();
	let partitions = catalog.list_ringbuffer_partitions(&mut txn, &rb).unwrap();

	assert_eq!(partitions.len(), 1, "expected only the still-live 'eu' partition's metadata, got {partitions:?}");
	assert_eq!(partitions[0].partition_values, vec![Value::Utf8("eu".to_string())]);
}

#[test]
fn test_ringbuffer_delete_partition_to_zero_then_refill_evicts_correctly() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(
		"CREATE RINGBUFFER test::rb { region: utf8, n: int4 } WITH { capacity: 2, partition: { by: { region } } }",
	);

	t.command(r#"INSERT test::rb [{ region: "us", n: 1 }, { region: "us", n: 2 }, { region: "eu", n: 100 }]"#);

	t.command(r#"DELETE test::rb FILTER { region == "us" }"#);

	let frames = t.query(r#"FROM test::rb FILTER { region == "us" }"#);
	assert_eq!(frames[0].rows().count(), 0);

	t.command(r#"INSERT test::rb [{ region: "us", n: 3 }, { region: "us", n: 4 }, { region: "us", n: 5 }]"#);

	let frames = t.query(r#"FROM test::rb FILTER { region == "us" } SORT { n: asc }"#);
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 2);
	assert_eq!(rows[0].get::<i32>("n").unwrap().unwrap(), 4);
	assert_eq!(rows[1].get::<i32>("n").unwrap().unwrap(), 5);

	let frames = t.query(r#"FROM test::rb FILTER { region == "eu" }"#);
	assert_eq!(frames[0].rows().count(), 1);
}
