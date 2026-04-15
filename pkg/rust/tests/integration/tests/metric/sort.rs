// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{sync::Arc, thread, time::Duration};

use reifydb::{Database, Params, SharedRuntimeConfig, WithSubsystem, embedded as db_embedded};
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
};
use reifydb_sub_metric::factory::MetricSubsystemFactory;
use reifydb_type::value::frame::frame::Frame;

fn wait_for_metrics_processing() {
	thread::sleep(Duration::from_millis(150));
}

fn new_db_with_metrics() -> Database {
	let registry = Arc::new(MetricRegistry::new());
	let static_registry = Arc::new(StaticMetricRegistry::new());
	let accumulator = Arc::new(StatementStatsAccumulator::new());
	let factory = Box::new(MetricSubsystemFactory::new(registry, static_registry, accumulator));

	let mut db = db_embedded::memory()
		.with_runtime_config(SharedRuntimeConfig::default().seeded(0))
		.with_subsystem(factory)
		.build()
		.expect("build");
	db.start().expect("start");
	db
}

fn admin(db: &Database, rql: &str) -> Vec<Frame> {
	db.admin_as_root(rql, Params::None).expect("admin failed")
}

fn command(db: &Database, rql: &str) -> Vec<Frame> {
	db.command_as_root(rql, Params::None).expect("command failed")
}

fn query(db: &Database, rql: &str) -> Vec<Frame> {
	db.query_as_root(rql, Params::None).expect("query failed")
}

#[test]
fn test_sort_table_storage_stats_multiline_syntax() {
	let db = new_db_with_metrics();

	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::tiny { id: int4 }");
	admin(&db, "CREATE TABLE test::large { id: int4, name: text, description: text }");

	command(&db, r#"INSERT test::tiny [{ id: 1 }]"#);

	command(
		&db,
		r#"
		INSERT test::large [
			{ id: 1, name: "abcdefghij", description: "This is a longer description with more text" },
			{ id: 2, name: "klmnopqrst", description: "Another long description with lots of data" },
			{ id: 3, name: "uvwxyzabcd", description: "Yet another description to increase size" }
		]
	"#,
	);

	wait_for_metrics_processing();

	let multiline_query = "from system::table_storage_stats
sort {total_bytes:asc}";

	let frames = query(&db, multiline_query);

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

	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::a { id: int4 }");
	admin(&db, "CREATE TABLE test::b { id: int4, data: text }");

	command(&db, r#"INSERT test::a [{ id: 1 }]"#);
	command(
		&db,
		r#"
		INSERT test::b [
			{ id: 1, data: "lots of data here to make this bigger" },
			{ id: 2, data: "even more data to increase size further" },
			{ id: 3, data: "yet more data to make this the largest" }
		]
	"#,
	);

	wait_for_metrics_processing();

	let frames_asc = query(&db, "from system::table_storage_stats\nsort {total_bytes:asc}");
	let frames_desc = query(&db, "from system::table_storage_stats\nsort {total_bytes:desc}");

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

	admin(&db, "CREATE NAMESPACE test");
	admin(&db, "CREATE TABLE test::tiny { id: int4 }");
	admin(&db, "CREATE TABLE test::small { id: int4, name: text }");
	admin(&db, "CREATE TABLE test::medium { id: int4, name: text }");
	admin(&db, "CREATE TABLE test::large { id: int4, name: text, description: text }");

	command(&db, r#"INSERT test::tiny [{ id: 1 }]"#);
	command(&db, r#"INSERT test::small [{ id: 1, name: "a" }]"#);
	command(
		&db,
		r#"
		INSERT test::medium [
			{ id: 1, name: "abc" },
			{ id: 2, name: "def" },
			{ id: 3, name: "ghi" }
		]
	"#,
	);
	command(
		&db,
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

	let frames_asc = query(&db, "FROM system::table_storage_stats SORT {total_bytes:ASC}");

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

	let frames_desc = query(&db, "FROM system::table_storage_stats SORT {total_bytes:DESC}");

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
