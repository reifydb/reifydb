// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{sync::Arc, thread};

use reifydb::{ConfigKey, RuntimeConfig, Value, embedded as db_embedded, value::value::duration::Duration};
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_test_harness::db::TestDb;

fn wait_for_metrics_processing() {
	thread::sleep(Duration::from_milliseconds(150).unwrap().to_std());
}

fn new_db_with_metrics() -> TestDb {
	let registry = Arc::new(MetricRegistry::new());
	let static_registry = Arc::new(StaticMetricRegistry::new());
	let accumulator = Arc::new(StatementStatsAccumulator::new());

	// The metric subsystem is wired unconditionally by DatabaseBuilder; it activates its
	// accounting path by resolving these registries from the IoC container, so inject them
	// rather than constructing a second MetricSubsystemFactory (which would double-register
	// the runtime vtables).
	TestDb::from(
		db_embedded::memory()
			.with_runtime_config(RuntimeConfig::default().seeded(0))
			// Seed a fast flush interval so the collector populates system::metrics::storage::table::current
			// well within wait_for_metrics_processing(); the default 10s cadence would leave it empty.
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_dependency(registry)
			.with_dependency(static_registry)
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

	wait_for_metrics_processing();

	let multiline_query = "from system::metrics::storage::table::current
sort {total_bytes:asc}";

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

	wait_for_metrics_processing();

	let frames_asc = db.query("from system::metrics::storage::table::current\nsort {total_bytes:asc}");
	let frames_desc = db.query("from system::metrics::storage::table::current\nsort {total_bytes:desc}");

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

	wait_for_metrics_processing();

	let frames_asc = db.query("FROM system::metrics::storage::table::current SORT {total_bytes:ASC}");

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

	let frames_desc = db.query("FROM system::metrics::storage::table::current SORT {total_bytes:DESC}");

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
