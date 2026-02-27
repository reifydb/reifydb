// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::value::{frame::frame::Frame, identity::IdentityId};

fn test_identity() -> IdentityId {
	IdentityId::root()
}

fn create_namespace(engine: &reifydb_engine::engine::StandardEngine, name: &str) {
	let identity = test_identity();
	engine.admin_as(identity, &format!("CREATE NAMESPACE {name}"), Default::default()).unwrap();
}

fn create_table(engine: &reifydb_engine::engine::StandardEngine, namespace: &str, table: &str, columns: &str) {
	let identity = test_identity();
	engine.admin_as(identity, &format!("CREATE TABLE {namespace}::{table} {{ {columns} }}"), Default::default())
		.unwrap();
}

fn query_table(engine: &reifydb_engine::engine::StandardEngine, table: &str) -> Vec<Frame> {
	let identity = test_identity();
	engine.query_as(identity, &format!("FROM {table}"), Default::default()).unwrap()
}

fn row_count(frames: &[Frame]) -> usize {
	frames.first().unwrap().rows().count()
}

#[test]
fn test_positional_insert_basic() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "users", "id: int4, name: utf8");

	engine.command_as(identity, r#"INSERT test::users [(1, "Alice"), (2, "Bob")]"#, Default::default()).unwrap();

	let frames = query_table(&engine, "test::users");
	assert_eq!(row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<i32>("id").unwrap().unwrap(), r.get::<String>("name").unwrap().unwrap()))
		.collect();
	values.sort_by_key(|(id, _)| *id);
	assert_eq!(values, vec![(1, "Alice".to_string()), (2, "Bob".to_string())]);
}

#[test]
fn test_positional_insert_single_row() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "items", "id: int4, value: float8");

	engine.command_as(identity, r#"INSERT test::items [(1, 10.5)]"#, Default::default()).unwrap();

	let frames = query_table(&engine, "test::items");
	assert_eq!(row_count(&frames), 1);

	let row = frames[0].rows().next().unwrap();
	assert_eq!(row.get::<i32>("id").unwrap(), Some(1));
	assert_eq!(row.get::<f64>("value").unwrap(), Some(10.5));
}

#[test]
fn test_positional_insert_wrong_column_count() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "data", "id: int4, name: utf8, active: bool");

	let result = engine.command_as(identity, r#"INSERT test::data [(1, "Alice")]"#, Default::default());
	assert!(result.is_err());
}

#[test]
fn test_positional_insert_multiline() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "records", "id: int4, name: utf8, active: bool");

	engine.command_as(
		identity,
		r#"
			INSERT test::records [
				(1, "Alice", true),
				(2, "Bob", false),
				(3, "Charlie", true)
			]
			"#,
		Default::default(),
	)
	.unwrap();

	let frames = query_table(&engine, "test::records");
	assert_eq!(row_count(&frames), 3);
}

#[test]
fn test_keyed_insert_still_works() {
	let engine = create_test_engine();
	let identity = test_identity();

	create_namespace(&engine, "test");
	create_table(&engine, "test", "users", "id: int4, name: utf8");

	engine.command_as(identity, r#"INSERT test::users [{ id: 1, name: "Alice" }]"#, Default::default()).unwrap();

	let frames = query_table(&engine, "test::users");
	assert_eq!(row_count(&frames), 1);
}
