// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::collections::BTreeMap;

use reifydb::{
	Clock, Frame, MockClock, RuntimeConfig, WithSubsystem, embedded as db_embedded,
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
fn series_without_a_sealed_bucket_raises_query_012() {
	// Nothing was inserted, so no bucket ever sealed and the series has no block to read. Coming
	// back empty would be indistinguishable from a series whose rows are simply not materialized
	// yet, and would let a stale read pass as a complete one.
	let db = TestDb::memory();
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");

	let err = column_query(&db, "from test::s").error.expect("a series with no sealed bucket must fail");
	assert_eq!(err.code, "QUERY_012");
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

const SERIES_BUCKET_WIDTH: u64 = 10;
const SERIES_GRACE_MS: i64 = 500;
const MOCK_EPOCH_NANOS: u64 = 1_700_000_000_000_000_000;

fn series_db() -> TestDb {
	// The clock is frozen so an integer keyed series seals only on the key rule, which is
	// deterministic. A sealing test opts into the grace path by advancing it explicitly; a real
	// clock would make that path fire whenever the machine was slow and pass for the wrong reason.
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_bucket_width: SERIES_BUCKET_WIDTH,
		series_grace: Duration::from_milliseconds(SERIES_GRACE_MS).unwrap(),
	};
	let db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(config)))
			.with_runtime_config(RuntimeConfig::default().clock(Clock::Mock(MockClock::new(MOCK_EPOCH_NANOS))))
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db
}

fn insert_keys(db: &TestDb, target: &str, keys: impl IntoIterator<Item = u64>) {
	let rows: Vec<String> = keys.into_iter().map(|k| format!("{{ k: {k}, value: {k}.5 }}")).collect();
	db.command(&format!("INSERT {target} [{}]", rows.join(", ")));
}

fn insert_partition_keys(db: &TestDb, target: &str, region: &str, keys: impl IntoIterator<Item = u64>) {
	let rows: Vec<String> =
		keys.into_iter().map(|k| format!("{{ k: {k}, region: '{region}', n: {k} }}")).collect();
	db.command(&format!("INSERT {target} [{}]", rows.join(", ")));
}

fn uint8_cells(frames: &[Frame], name: &str) -> Vec<u64> {
	frames.iter()
		.flat_map(|frame| cells(frame, name))
		.map(|value| match value {
			Value::Uint8(k) => k,
			other => panic!("column {name}: expected Uint8, got {other:?}"),
		})
		.collect()
}

fn utf8_cells(frames: &[Frame], name: &str) -> Vec<String> {
	frames.iter()
		.flat_map(|frame| cells(frame, name))
		.map(|value| match value {
			Value::Utf8(s) => s,
			other => panic!("column {name}: expected Utf8, got {other:?}"),
		})
		.collect()
}

fn plain_series(db: &TestDb) {
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");
}

#[test]
fn sealed_series_buckets_come_back_in_key_order() {
	// Series keys are stored descending, so a single row store range scan returns newest first.
	// The column path reads one block per bucket and must put them back in that same order, or
	// the two entry points disagree on ordering while every row level assertion still passes.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=25);

	let result = await_column_rows(&db, "from test::s", 19);
	let keys = uint8_cells(&result.frames, "k");

	assert_eq!(keys, (1..=19).rev().collect::<Vec<u64>>());
}

#[test]
fn the_open_series_bucket_is_absent_from_the_column_path() {
	// An open bucket is still taking writes, so publishing it would let a later insert change
	// rows a reader has already seen. Asserting only that the sealed keys are present would
	// pass while the open ones leaked in alongside them.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=25);

	let result = await_column_rows(&db, "from test::s", 19);
	let keys = uint8_cells(&result.frames, "k");

	assert!(keys.contains(&19), "the last key of the sealed bucket must be present");
	for open in 20..=25 {
		assert!(!keys.contains(&open), "key {open} lives in the open bucket and must not be readable");
	}
}

#[test]
fn each_sealed_bucket_carries_its_own_commit_version() {
	// The version is stamped per block, not per query. An implementation that stamps the newest
	// snapshot's version across every block reports rows as fresher than they are, and a test
	// that only checks the values are non-zero would pass on exactly that bug.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=10);
	await_column_rows(&db, "from test::s", 9);
	insert_keys(&db, "test::s", 11..=20);

	let result = await_column_rows(&db, "from test::s", 19);
	let mut versions = cells(&result.frames[0], "#commit_version");
	for frame in &result.frames[1..] {
		versions.extend(cells(frame, "#commit_version"));
	}
	versions.sort();
	versions.dedup();

	assert_eq!(versions.len(), 2, "two buckets sealed at two commits must carry two versions: {versions:?}");
}

#[test]
fn a_series_whose_only_bucket_is_open_raises_query_012_naming_the_series() {
	// Rows exist but none are readable yet. Returning them empty would be indistinguishable from
	// a series that genuinely holds nothing, so the diagnostic has to fire and has to say which
	// object it is about.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=5);

	let err = column_query(&db, "from test::s").error.expect("an unsealed bucket must fail, not come back empty");
	assert_eq!(err.code, "QUERY_012");
	assert!(err.message.contains("test::s"), "the diagnostic must name the series: {}", err.message);
}

#[test]
fn series_headers_are_the_key_the_data_columns_and_commit_version() {
	// The frame is built from the block schema, so a header list that drifts from it hands back
	// columns whose names do not match their data.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=25);

	let result = await_column_rows(&db, "from test::s", 19);

	assert_eq!(column_names(&result.frames[0]), vec!["k", "value", "#commit_version"]);
}

#[test]
fn a_tagged_series_puts_the_tag_column_after_the_key() {
	// The block names the discriminant column "tag" regardless of the declared sum type, and
	// places it between the key and the data columns. Any other position silently pairs every
	// data column with the wrong name.
	let db = series_db();
	db.admin("CREATE ENUM test::status { Active, Inactive }");
	db.admin("CREATE SERIES test::tg { k: uint8, value: float8 } WITH { key: k, tag: test::status }");
	let rows: Vec<String> =
		(1..=25).map(|k| format!("{{ k: {k}, value: {k}.5, tag: {} }}", if k % 2 == 0 { 1 } else { 0 })).collect();
	db.command(&format!("INSERT test::tg [{}]", rows.join(", ")));

	let result = await_column_rows(&db, "from test::tg", 19);

	assert_eq!(column_names(&result.frames[0]), vec!["k", "tag", "value", "#commit_version"]);
}

#[test]
fn a_variant_tag_filter_returns_only_that_tag() {
	// The tag clause is translated against the block's "tag" column. Naming it after the sum
	// type instead finds no column, and the scan then returns every variant.
	let db = series_db();
	db.admin("CREATE ENUM test::status { Active, Inactive }");
	db.admin("CREATE SERIES test::tg { k: uint8, value: float8 } WITH { key: k, tag: test::status }");
	let rows: Vec<String> =
		(1..=25).map(|k| format!("{{ k: {k}, value: {k}.5, tag: {} }}", if k % 2 == 0 { 1 } else { 0 })).collect();
	db.command(&format!("INSERT test::tg [{}]", rows.join(", ")));
	await_column_rows(&db, "from test::tg", 19);

	let result = await_column_rows(&db, "from test::tg filter tag == 0", 10);
	let keys = uint8_cells(&result.frames, "k");

	assert_eq!(keys, vec![19, 17, 15, 13, 11, 9, 7, 5, 3, 1]);
}

#[test]
fn a_key_range_trims_rows_inside_the_edge_bucket() {
	// A range starting mid bucket cannot be answered by skipping blocks: the bucket holding the
	// boundary has to be opened and its lower rows dropped. Pruning alone returns keys 10 to 14
	// as well, which is a wrong answer rather than a slow one.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=25);
	await_column_rows(&db, "from test::s", 19);

	let result = await_column_rows(&db, "from test::s filter k >= 15", 5);
	let keys = uint8_cells(&result.frames, "k");

	assert_eq!(keys, vec![19, 18, 17, 16, 15]);
}

#[test]
fn a_key_range_prunes_the_buckets_outside_it() {
	// A window inside one bucket must exclude every key of the neighbouring buckets. Checking
	// only the count would pass if the range were applied with the bounds swapped.
	let db = series_db();
	plain_series(&db);
	insert_keys(&db, "test::s", 1..=25);
	await_column_rows(&db, "from test::s", 19);

	let result = await_column_rows(&db, "from test::s filter k >= 12 and k < 16", 4);
	let keys = uint8_cells(&result.frames, "k");

	assert_eq!(keys, vec![15, 14, 13, 12]);
}

#[test]
fn a_partition_filter_returns_only_that_partition() {
	// Every partition's blocks live under the same series, so a scan that ignores the partition
	// returns another partition's rows under the asked partition's name. The other partition
	// carries values that are obviously wrong if they appear.
	let db = series_db();
	db.admin("CREATE SERIES test::p { k: uint8, region: utf8, n: int4 } WITH { key: k, partition: { by: { region } } }");
	insert_partition_keys(&db, "test::p", "us", 1..=25);
	insert_partition_keys(&db, "test::p", "eu", 1..=25);
	await_column_rows(&db, "from test::p", 38);

	let result = await_column_rows(&db, "from test::p filter region == 'us'", 19);
	let regions = utf8_cells(&result.frames, "region");

	assert!(!regions.is_empty(), "the filtered scan must return rows, not an empty frame");
	assert!(regions.iter().all(|r| r == "us"), "another partition's rows leaked in: {regions:?}");
}

#[test]
fn a_quiet_partition_seals_once_grace_elapses() {
	// An integer key carries no wall clock meaning, so a partition that stops writing mid bucket
	// has no key based path to sealing. Without the grace backstop these rows stay unreadable
	// forever, which is the failure this design introduced.
	let db = series_db();
	db.admin("CREATE SERIES test::p { k: uint8, region: utf8, n: int4 } WITH { key: k, partition: { by: { region } } }");
	insert_partition_keys(&db, "test::p", "us", 1..=5);

	let err = column_query(&db, "from test::p").error.expect("a bucket below the key rule must not seal on its own");
	assert_eq!(err.code, "QUERY_012");

	db.mock_clock().advance_millis((SERIES_GRACE_MS * 2) as u64);

	let result = await_column_rows(&db, "from test::p", 5);
	assert_eq!(uint8_cells(&result.frames, "k"), vec![5, 4, 3, 2, 1]);
}

#[test]
fn a_fast_partition_does_not_seal_a_slow_one() {
	// The key rule reads the writing partition's own newest key. Reading it from shared series
	// state instead would let a busy partition seal a quiet one's live bucket, publishing rows
	// that are still being written to. The clock never moves, so the quiet partition can only
	// seal through that bug.
	let db = series_db();
	db.admin("CREATE SERIES test::p { k: uint8, region: utf8, n: int4 } WITH { key: k, partition: { by: { region } } }");
	insert_partition_keys(&db, "test::p", "us", 1..=25);
	insert_partition_keys(&db, "test::p", "eu", 1..=5);

	let result = await_column_rows(&db, "from test::p", 19);
	let regions = utf8_cells(&result.frames, "region");

	assert!(regions.iter().all(|r| r == "us"), "the quiet partition's open bucket was published: {regions:?}");
}

