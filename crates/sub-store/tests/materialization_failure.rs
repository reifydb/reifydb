// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "column", reifydb_target = "host"))]

use std::{
	env,
	os::unix::process::ExitStatusExt,
	process::{Command, Output},
	sync::Arc,
};

use reifydb::{
	embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_runtime::io::fs::memory::MemoryFs;
use reifydb_store_column::testing::NoFaults;
use reifydb_sub_store::subsystem::StorageConfig;
use reifydb_test_harness::fixture::column::{FailWrites, memory_store};
use reifydb_value::value::duration::Duration;

const CHILD: &str = "REIFYDB_SUB_STORE_FAILURE_CHILD";
const SIGABRT: i32 = 6;

fn is_child() -> bool {
	env::var(CHILD).is_ok()
}

fn run_child(test_name: &str) -> Output {
	// The child is armed so a panic aborts it exactly as it would abort a production process.
	Command::new(env::current_exe().expect("the test binary must be locatable to re-enter it"))
		.args(["--exact", test_name, "--nocapture"])
		.env(CHILD, "1")
		.env("REIFYDB_FATAL", "1")
		.output()
		.expect("the child test process must start")
}

fn db_whose_column_store_cannot_persist(table_tick: Duration, series_tick: Duration) -> TestDb {
	// Every block write must fail with out of space, as it would on a full disk.
	let config = StorageConfig {
		table_tick_interval: table_tick,
		series_tick_interval: series_tick,
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
		..StorageConfig::default()
	};
	let store =
		memory_store(MemoryFs::new(), Arc::new(FailWrites), Arc::new(NoFaults)).expect("build column store");
	TestDb::from(db_embedded::memory().with_storage_config(config).with_column_store(store).build().expect("build"))
}

fn stay_alive_then_stop(mut db: TestDb) {
	// Without the abort the child reaches here and exits cleanly, which the parent must read as the bug.
	let _ = poll_until(|| None::<()>, Duration::from_seconds(3).unwrap().to_std());
	db.stop();
}

#[test]
fn a_failed_table_materialization_stops_the_process_and_names_the_table() {
	// Without the abort a warn-and-retry keeps the process up while no table block is ever persisted again.
	if is_child() {
		let db = db_whose_column_store_cannot_persist(
			Duration::from_milliseconds(50).unwrap(),
			Duration::from_seconds(3600).unwrap(),
		);
		db.admin("CREATE NAMESPACE test");
		db.admin("CREATE TABLE test::t { id: int4 }");
		db.command("INSERT test::t [{id: 1}]");
		stay_alive_then_stop(db);
		return;
	}

	let output = run_child("a_failed_table_materialization_stops_the_process_and_names_the_table");
	let stderr = String::from_utf8_lossy(&output.stderr);

	assert_eq!(
		output.status.signal(),
		Some(SIGABRT),
		"a table materialization failure must abort the process; status {:?}, stderr:\n{}",
		output.status,
		stderr
	);
	assert!(
		stderr.contains("table materialization failed for table"),
		"the report must name the table that failed; stderr:\n{}",
		stderr
	);
	assert!(stderr.contains("out of space"), "the report must carry the underlying cause; stderr:\n{}", stderr);
}

#[test]
fn a_failed_series_materialization_stops_the_process_and_names_the_series() {
	// Without its own pin the series path can swallow the failure while the table case stays green.
	if is_child() {
		let db = db_whose_column_store_cannot_persist(
			Duration::from_seconds(3600).unwrap(),
			Duration::from_milliseconds(50).unwrap(),
		);
		db.admin("CREATE NAMESPACE test");
		db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");
		let rows: Vec<String> = (0u64..=11).map(|k| format!("{{k: {k}, value: {k}.0}}")).collect();
		db.command(&format!("INSERT test::s [{}]", rows.join(", ")));
		stay_alive_then_stop(db);
		return;
	}

	let output = run_child("a_failed_series_materialization_stops_the_process_and_names_the_series");
	let stderr = String::from_utf8_lossy(&output.stderr);

	assert_eq!(
		output.status.signal(),
		Some(SIGABRT),
		"a series materialization failure must abort the process; status {:?}, stderr:\n{}",
		output.status,
		stderr
	);
	assert!(
		stderr.contains("series materialization failed for series") && stderr.contains("(s)"),
		"the report must name series s; stderr:\n{}",
		stderr
	);
	assert!(stderr.contains("out of space"), "the report must carry the underlying cause; stderr:\n{}", stderr);
}
