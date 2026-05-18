// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::{Database, Params, SharedRuntimeConfig, embedded as db_embedded};
use reifydb_type::value::frame::frame::Frame;

fn new_db() -> Database {
	let mut db = db_embedded::memory()
		.with_runtime_config(SharedRuntimeConfig::default().seeded(0))
		.build()
		.expect("build");
	db.start().expect("start");
	db
}

fn admin(db: &Database, rql: &str) -> Vec<Frame> {
	db.admin_as_root(rql, Params::None).expect("admin failed")
}

fn query(db: &Database, rql: &str) -> Vec<Frame> {
	db.query_as_root(rql, Params::None).expect("query failed")
}

#[test]
fn test_sort_system_namespaces() {
	let db = new_db();

	admin(&db, "CREATE NAMESPACE zoo");
	admin(&db, "CREATE NAMESPACE alpha");
	admin(&db, "CREATE NAMESPACE beta");

	let frames = query(&db, "FROM system::namespaces SORT {name}");

	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	for i in 1..names.len() {
		assert!(
			names[i - 1] >= names[i],
			"Names should be sorted in descending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[test]
fn test_sort_system_namespaces_asc() {
	let db = new_db();

	admin(&db, "CREATE NAMESPACE zoo");
	admin(&db, "CREATE NAMESPACE alpha");
	admin(&db, "CREATE NAMESPACE beta");

	let frames = query(&db, "FROM system::namespaces SORT {name:ASC}");

	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	for i in 1..names.len() {
		assert!(
			names[i - 1] <= names[i],
			"Names should be sorted in ascending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[test]
fn test_sort_system_tables() {
	let db = new_db();

	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::zebra { id: int4 }");
	admin(&db, "CREATE TABLE test::apple { id: int4 }");
	admin(&db, "CREATE TABLE test::banana { id: int4 }");

	let frames = query(&db, "FROM system::tables SORT {name:ASC}");

	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	for i in 1..names.len() {
		assert!(
			names[i - 1] <= names[i],
			"Names should be sorted in ascending order, but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}

#[test]
fn test_sort_system_tables_with_pipe_syntax() {
	let db = new_db();

	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::zebra { id: int4 }");
	admin(&db, "CREATE TABLE test::apple { id: int4 }");
	admin(&db, "CREATE TABLE test::banana { id: int4 }");

	let frames = query(&db, "from system::tables | sort {name}");

	let frame = frames.first().expect("Expected at least one frame");
	let name_column = frame.columns.iter().find(|col| col.name == "name").expect("Expected 'name' column");

	let row_count = name_column.data.len();
	let mut names: Vec<String> = Vec::new();
	for i in 0..row_count {
		names.push(name_column.data.as_string(i));
	}

	for i in 1..names.len() {
		assert!(
			names[i - 1] >= names[i],
			"Names should be sorted in descending order (default), but '{}' comes before '{}'",
			names[i - 1],
			names[i]
		);
	}
}
