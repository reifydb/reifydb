// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;

#[test]
fn test_positional_insert_basic() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::users { id: int4, name: utf8 }");

	t.command(r#"INSERT test::users [(1, "Alice"), (2, "Bob")]"#);

	let frames = t.query("FROM test::users");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);
}

#[test]
fn test_positional_insert_single_row() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int4, value: float8 }");

	t.command(r#"INSERT test::items [(1, 10.5)]"#);

	let frames = t.query("FROM test::items");
	assert_eq!(TestEngine::row_count(&frames), 1);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap(), Some(1));
	assert_eq!(row.get::<f64>("value").unwrap(), Some(10.5));
}

#[test]
fn test_positional_insert_wrong_column_count() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::data { id: int4, name: utf8, active: bool }");

	let result = t.command_as(IdentityId::system(), r#"INSERT test::data [(1, "Alice")]"#, Default::default());
	assert!(result.is_err());
}

#[test]
fn test_positional_insert_multiline() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::records { id: int4, name: utf8, active: bool }");

	t.command(
		r#"
			INSERT test::records [
				(1, "Alice", true),
				(2, "Bob", false),
				(3, "Charlie", true)
			]
			"#,
	);

	let frames = t.query("FROM test::records");
	assert_eq!(TestEngine::row_count(&frames), 3);
}

#[test]
fn test_keyed_insert_still_works() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::users { id: int4, name: utf8 }");

	t.command(r#"INSERT test::users [{ id: 1, name: "Alice" }]"#);

	let frames = t.query("FROM test::users");
	assert_eq!(TestEngine::row_count(&frames), 1);
}
