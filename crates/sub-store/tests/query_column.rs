// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::collections::BTreeMap;

use reifydb::{
	Frame, WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::execution::ExecutionResult;
use reifydb_sub_store::{factory::StorageSubsystemFactory, subsystem::StorageConfig};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	params::Params,
	value::{Value, duration::Duration, identity::IdentityId},
};

const INSERT_THREE: &str = "INSERT test::t [{id: 1, name: \"alpha\", score: 1.5},\
	 {id: 2, name: \"bravo\", score: 2.5},\
	 {id: 3, name: \"charlie\", score: 3.5}]";

fn materializing_db() -> TestDb {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};
	let db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db
}

fn column_query(db: &TestDb, rql: &str) -> ExecutionResult {
	db.engine().query_column_as(IdentityId::root(), rql, Params::None)
}

fn await_column_rows(db: &TestDb, rql: &str, want: usize) -> ExecutionResult {
	poll_until(
		|| {
			let result = column_query(db, rql);
			let rows: usize = result.frames.iter().map(|f| f.row_count()).sum();
			(result.error.is_none() && rows == want).then_some(result)
		},
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.unwrap_or_else(|| panic!("query_column never returned {want} rows for `{rql}` within 5 seconds"))
}

fn column_names(frame: &Frame) -> Vec<String> {
	frame.columns.iter().map(|c| c.name.clone()).collect()
}

fn cells(frame: &Frame, name: &str) -> Vec<Value> {
	let column = frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("column {name} missing"));
	(0..column.data.len()).map(|i| column.data.get_value(i)).collect()
}

fn rows_by_id(frames: &[Frame]) -> BTreeMap<i32, (String, f64)> {
	let mut out = BTreeMap::new();
	for frame in frames {
		let ids = cells(frame, "id");
		let names = cells(frame, "name");
		let scores = cells(frame, "score");
		for i in 0..ids.len() {
			let id = match &ids[i] {
				Value::Int4(v) => *v,
				other => panic!("row {i}: expected Int4, got {other:?}"),
			};
			let name = match &names[i] {
				Value::Utf8(s) => s.clone(),
				other => panic!("row {i}: expected Utf8, got {other:?}"),
			};
			let score = match &scores[i] {
				Value::Float8(v) => f64::from(*v),
				other => panic!("row {i}: expected Float8, got {other:?}"),
			};
			out.insert(id, (name, score));
		}
	}
	out
}

fn latest_snapshot_version(db: &TestDb, table: &str) -> u64 {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), table).expect("find table").expect("table");
	let snapshot = catalog
		.find_latest_column_snapshot_for_table(&mut tx, table.id)
		.expect("find snapshot")
		.expect("snapshot recorded");
	snapshot.read_version().0
}

#[test]
fn column_scan_returns_the_same_rows_as_the_row_scan() {
	// The row store is the source of truth, so the column store must reproduce it row for row.
	let db = materializing_db();
	db.admin("CREATE TABLE test::t { id: int4, name: utf8, score: float8 }");
	db.command(INSERT_THREE);

	let column = await_column_rows(&db, "from test::t", 3);
	let row = db.query("from test::t");

	let from_columns = rows_by_id(&column.frames);
	assert_eq!(from_columns.len(), 3, "every row must arrive exactly once");
	assert_eq!(from_columns, rows_by_id(&row));
}

#[test]
fn column_scan_headers_are_the_row_scan_headers_plus_commit_version() {
	// A drifting header would make the two entry points return differently shaped frames.
	let db = materializing_db();
	db.admin("CREATE TABLE test::t { id: int4, name: utf8, score: float8 }");
	db.command(INSERT_THREE);

	let column = await_column_rows(&db, "from test::t", 3);
	let row = db.query("from test::t");

	let mut expected = column_names(&row[0]);
	expected.push("#commit_version".to_string());
	assert_eq!(column_names(&column.frames[0]), expected);
}

#[test]
fn commit_version_column_equals_the_snapshot_read_version() {
	// The column names the version the rows reflect; any other value makes staleness invisible.
	let db = materializing_db();
	db.admin("CREATE TABLE test::t { id: int4, name: utf8, score: float8 }");
	db.command(INSERT_THREE);

	let column = await_column_rows(&db, "from test::t", 3);
	let recorded = latest_snapshot_version(&db, "t");

	let versions = cells(&column.frames[0], "#commit_version");
	assert_eq!(versions.len(), 3);
	for (i, v) in versions.iter().enumerate() {
		assert_eq!(v, &Value::Uint8(recorded), "row {i} carries a version other than the snapshot's");
	}
}

#[test]
fn empty_table_keeps_its_headers_on_the_column_path() {
	// The row path emits one empty typed batch so headers survive; the column path must agree.
	let db = materializing_db();
	db.admin("CREATE TABLE test::t { id: int4, name: utf8, score: float8 }");

	let column = await_column_rows(&db, "from test::t", 0);
	let row = db.query("from test::t");

	assert_eq!(column.frames.len(), 1, "an empty result still yields one frame");
	let mut expected = column_names(&row[0]);
	expected.push("#commit_version".to_string());
	assert_eq!(column_names(&column.frames[0]), expected);
}

#[test]
fn missing_snapshot_raises_query_012_naming_the_table() {
	// Returning an empty result here would read as "the table has no rows", which is wrong.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4 }");
	db.command("INSERT test::t [{id: 1}]");

	let result = column_query(&db, "from test::t");

	let err = result.error.expect("a table without a snapshot must fail, not come back empty");
	assert_eq!(err.code, "QUERY_012");
	assert!(err.message.contains("test::t"), "the diagnostic must name the table: {}", err.message);
	assert!(result.frames.is_empty(), "no partial frames may accompany the error");
}

#[test]
fn series_scan_is_rejected_instead_of_reading_the_row_store() {
	// A silent row-store read would break the promise that query_column touches only the column store.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");

	let err = column_query(&db, "from test::s").error.expect("series scans are not supported on the column path");
	assert_eq!(err.code, "QUERY_013");
}

#[test]
fn partition_scan_is_rejected_because_blocks_hold_every_partition() {
	// The block was built with no partition filter, so honouring one would silently return other partitions' rows.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::pt { ts: int8, region: utf8, n: int4 } WITH { partition: { by: { region } } }");
	db.command("INSERT test::pt [{ ts: 10, region: 'us', n: 1 }, { ts: 30, region: 'eu', n: 3 }]");

	let err = column_query(&db, "from test::pt filter region == 'us'")
		.error
		.expect("a partition scan must fail on the column path");
	assert_eq!(err.code, "QUERY_013");
}

#[test]
fn rownum_lookup_is_rejected_instead_of_reading_the_row_store() {
	// The planner rewrites `filter #rownum == n` into a row-store point lookup that bypasses the table scan arm.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4 }");
	db.command("INSERT test::t [{id: 1}]");

	let result = column_query(&db, "from test::t filter #rownum == 1");

	let err = result.error.expect("a rownum lookup must not silently read the row store");
	assert_eq!(err.code, "QUERY_013");
}
