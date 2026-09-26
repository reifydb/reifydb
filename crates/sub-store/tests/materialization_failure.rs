// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

use std::{
	env,
	os::unix::process::ExitStatusExt,
	process::{Command, Output},
};

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::interface::catalog::id::ColumnSnapshotId;
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
};
use reifydb_store_column::persistent::sqlite::SqliteColumnStore;
use reifydb_sub_store::{factory::StorageSubsystemFactory, subsystem::StorageConfig};
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

fn execute_on_column_db(config: &SqliteConfig, statement: &str) {
	let conn = connect(&resolve_db_path(config.path.clone(), "column.db"), convert_flags(&config.flags))
		.expect("open column.db from a second connection");
	conn.busy_timeout(Duration::from_seconds(5).unwrap().to_std()).expect("set busy timeout");
	conn.execute_batch(statement).expect("run statement on column.db");
}

fn db_whose_column_store_cannot_persist(table_tick: Duration, series_tick: Duration) -> (TestDb, SqliteTempPathGuard) {
	// Without the backing table every later block persist must fail, as it would on a lost or corrupt disk.
	let (column_cfg, guard) = SqliteConfig::in_memory();
	let config = StorageConfig {
		table_tick_interval: table_tick,
		series_tick_interval: series_tick,
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
		..StorageConfig::default()
	};
	let factory = StorageSubsystemFactory::new(config).with_column_sqlite(Some(column_cfg.clone()));
	let db = TestDb::from(db_embedded::memory().with_subsystem(Box::new(factory)).build().expect("build"));
	execute_on_column_db(&column_cfg, "DROP TABLE column_blocks");
	(db, guard)
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
		let (db, _guard) = db_whose_column_store_cannot_persist(
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
	assert!(
		stderr.contains("Failed to put column block"),
		"the report must carry the underlying cause; stderr:\n{}",
		stderr
	);
}

#[test]
fn a_failed_series_materialization_stops_the_process_and_names_the_series() {
	// Without its own pin the series path can swallow the failure while the table case stays green.
	if is_child() {
		let (db, _guard) = db_whose_column_store_cannot_persist(
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
	assert!(
		stderr.contains("Failed to put column block"),
		"the report must carry the underlying cause; stderr:\n{}",
		stderr
	);
}

#[test]
fn a_malformed_persisted_key_fails_the_load_instead_of_being_skipped() {
	// Otherwise the store warms up missing a block the catalog still points at.
	let (config, _guard) = SqliteConfig::in_memory();
	let store = SqliteColumnStore::new(config.clone());
	store.put(ColumnSnapshotId(1), &[1]).expect("put a well-formed block");
	execute_on_column_db(&config, "INSERT INTO column_blocks (snapshot_id, data) VALUES (x'010203', x'00')");

	let err = store.load_all().expect_err("a 3-byte snapshot_id key must fail the load");

	assert!(err.to_string().contains("malformed 3-byte snapshot_id key"), "unexpected error: {err}");
}
