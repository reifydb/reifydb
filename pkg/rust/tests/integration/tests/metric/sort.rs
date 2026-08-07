// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, time::Duration};

use reifydb::testing::db::TestDb;
use reifydb::{ConfigKey, RuntimeConfig, Value, embedded as db_embedded};
use reifydb_sub_metrics::accumulator::StatementMetricsAccumulator;

const TIMEOUT: Duration = Duration::from_secs(10);

fn await_table_metrics(db: &TestDb, query: &str, want: usize) {
	// The sampler publishes asynchronously, so poll the very query under test until every written
	// table has surfaced; a fixed sleep would leave the sort assertions running on a short frame.
	let rows = db.await_row_count(query, want, TIMEOUT);
	assert_eq!(rows, want, "expected {} table rows in system::metrics::storage::current, got {}", want, rows);
}

fn new_db_with_metrics() -> TestDb {
	let accumulator = Arc::new(StatementMetricsAccumulator::new());

	// The metric subsystem is already wired by DatabaseBuilder and activates by resolving the
	// accumulator from IoC, so inject it rather than adding a second MetricsSubsystemFactory,
	// which would double-register the runtime vtables.
	TestDb::from(
		db_embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(0))
			// Seed a fast flush interval so the collector populates system::metrics::storage::current
			// well within wait_for_metrics_processing(); the default 10s cadence would leave it empty.
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.with_dependency(accumulator)
			.build()
			.expect("build"),
	)
}

#[test]
fn test_sort_table_storage_stats_multiline_syntax() {
	let db = new_db_with_metrics();

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::tiny { id: int4 }");
	db.admin("CREATE TABLE test::large { id: int4, name: text, description: text }");

	db.command(r#"INSERT test::tiny [{ id: 1 }]"#);

	db.command(
		r#"
		INSERT test::large [
			{ id: 1, name: "abcdefghij", description: "This is a longer description with more text" },
			{ id: 2, name: "klmnopqrst", description: "Another long description with lots of data" },
			{ id: 3, name: "uvwxyzabcd", description: "Yet another description to increase size" }
		]
	"#,
	);

	// The nine per-object metrics tables merged into system::metrics::storage::current, so the
	// table rows this test sorts are now selected by the object_kind dimension.
	let multiline_query = "from system::metrics::storage::current
filter {object_kind == 'table'}
sort {total_bytes:asc}";

	await_table_metrics(&db, multiline_query, 2);

	let frames = db.query(multiline_query);

	let frame = frames.first().expect("Expected at least one frame");
	let id_col = frame.columns.iter().find(|c| c.name == "id").unwrap();
	let bytes_col = frame.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut data: Vec<(u64, u64)> = Vec::new();
	for i in 0..id_col.data.len() {
		let id = id_col.data.as_string(i).parse::<u64>().unwrap_or(0);
		let bytes = bytes_col.data.as_string(i).parse::<u64>().unwrap_or(0);
		data.push((id, bytes));
	}

	let byte_values: Vec<u64> = data.iter().map(|(_, bytes)| *bytes).collect();

	let min_bytes = *byte_values.iter().min().unwrap();

	for i in 1..byte_values.len() {
		assert!(
			byte_values[i - 1] <= byte_values[i],
			"Multi-line ASC: Byte counts should be sorted in ascending order, but {} comes before {}",
			byte_values[i - 1],
			byte_values[i]
		);
	}

	assert_eq!(
		byte_values[0], min_bytes,
		"First value should be smallest for ASC sort, but got {} instead of {}",
		byte_values[0], min_bytes
	);
}

#[test]
fn test_asc_is_not_desc() {
	let db = new_db_with_metrics();

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::a { id: int4 }");
	db.admin("CREATE TABLE test::b { id: int4, data: text }");

	db.command(r#"INSERT test::a [{ id: 1 }]"#);
	db.command(
		r#"
		INSERT test::b [
			{ id: 1, data: "lots of data here to make this bigger" },
			{ id: 2, data: "even more data to increase size further" },
			{ id: 3, data: "yet more data to make this the largest" }
		]
	"#,
	);

	let asc = "from system::metrics::storage::current\nfilter {object_kind == 'table'}\nsort {total_bytes:asc}";
	let desc = "from system::metrics::storage::current\nfilter {object_kind == 'table'}\nsort {total_bytes:desc}";

	await_table_metrics(&db, asc, 2);

	let frames_asc = db.query(asc);
	let frames_desc = db.query(desc);

	let frame_asc = frames_asc.first().unwrap();
	let bytes_col_asc = frame_asc.columns.iter().find(|c| c.name == "total_bytes").unwrap();
	let first_asc = bytes_col_asc.data.as_string(0).parse::<u64>().unwrap();

	let frame_desc = frames_desc.first().unwrap();
	let bytes_col_desc = frame_desc.columns.iter().find(|c| c.name == "total_bytes").unwrap();
	let first_desc = bytes_col_desc.data.as_string(0).parse::<u64>().unwrap();

	assert_ne!(
		first_asc, first_desc,
		"ASC and DESC should return different first values, but both returned {}. ASC may be behaving like DESC!",
		first_asc
	);

	assert!(
		first_asc < first_desc,
		"ASC first value ({}) should be LESS than DESC first value ({}), but it's not! ASC is behaving like DESC.",
		first_asc,
		first_desc
	);
}

#[test]
fn test_sort_table_storage_stats_by_total_bytes() {
	let db = new_db_with_metrics();

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::tiny { id: int4 }");
	db.admin("CREATE TABLE test::small { id: int4, name: text }");
	db.admin("CREATE TABLE test::medium { id: int4, name: text }");
	db.admin("CREATE TABLE test::large { id: int4, name: text, description: text }");

	db.command(r#"INSERT test::tiny [{ id: 1 }]"#);
	db.command(r#"INSERT test::small [{ id: 1, name: "a" }]"#);
	db.command(
		r#"
		INSERT test::medium [
			{ id: 1, name: "abc" },
			{ id: 2, name: "def" },
			{ id: 3, name: "ghi" }
		]
	"#,
	);
	db.command(
		r#"
		INSERT test::large [
			{ id: 1, name: "abcdefghij", description: "This is a longer description with more text" },
			{ id: 2, name: "opqrstuvwx", description: "Fifth and final row with more text data" },
			{ id: 3, name: "klmnopqrst", description: "Another long description with lots of data" },
			{ id: 4, name: "uvwxyzabcd", description: "Yet another description to increase size" },
			{ id: 5, name: "efghijklmn", description: "Fourth row with substantial content here" }
		]
	"#,
	);

	let asc = "FROM system::metrics::storage::current FILTER {object_kind == 'table'} SORT {total_bytes:ASC}";

	await_table_metrics(&db, asc, 4);

	let frames_asc = db.query(asc);

	let frame_asc = frames_asc.first().expect("Expected at least one frame");
	let bytes_col_asc = frame_asc.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut byte_values_asc: Vec<u64> = Vec::new();
	for i in 0..bytes_col_asc.data.len() {
		byte_values_asc.push(bytes_col_asc.data.as_string(i).parse::<u64>().unwrap_or(0));
	}

	for i in 1..byte_values_asc.len() {
		assert!(
			byte_values_asc[i - 1] <= byte_values_asc[i],
			"ASC: Byte counts should be sorted in ascending order, but {} comes before {}",
			byte_values_asc[i - 1],
			byte_values_asc[i]
		);
	}

	let frames_desc = db.query(
		"FROM system::metrics::storage::current FILTER {object_kind == 'table'} SORT {total_bytes:DESC}",
	);

	let frame_desc = frames_desc.first().expect("Expected at least one frame");
	let bytes_col_desc = frame_desc.columns.iter().find(|c| c.name == "total_bytes").unwrap();

	let mut byte_values_desc: Vec<u64> = Vec::new();
	for i in 0..bytes_col_desc.data.len() {
		byte_values_desc.push(bytes_col_desc.data.as_string(i).parse::<u64>().unwrap_or(0));
	}

	for i in 1..byte_values_desc.len() {
		assert!(
			byte_values_desc[i - 1] >= byte_values_desc[i],
			"DESC: Byte counts should be sorted in descending order, but {} comes before {}",
			byte_values_desc[i - 1],
			byte_values_desc[i]
		);
	}

	let mut asc_reversed = byte_values_asc.clone();
	asc_reversed.reverse();
	assert_eq!(byte_values_desc, asc_reversed, "DESC sort should be reverse of ASC sort");
}
