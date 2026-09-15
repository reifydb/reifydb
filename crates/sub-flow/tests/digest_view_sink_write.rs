// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_sub_api::subsystem::HealthStatus;

const TIMEOUT: Duration = Duration::from_secs(10);

const DIGEST: &str = "aggregate { d: stats::digest(latency, 0.01) }";

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { ts: int8, g: int4, latency: float8 }");
	db
}

fn insert_rows(db: &TestDb) {
	db.command("INSERT app::t [{ ts: 1, g: 1, latency: 1.5 }, { ts: 2, g: 2, latency: 3.0 }]");
}

fn failed_sink_writes(db: &TestDb, expected: usize) -> String {
	let deadline = Instant::now() + TIMEOUT;
	loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status && description.starts_with(&format!("{expected} deferred flow(s) poisoned"))
		{
			return description.clone();
		}
		assert!(
			Instant::now() < deadline,
			"{expected} flows must fail their sink write, last status: {status:?}"
		);
		sleep(Duration::from_millis(20));
	}
}

fn assert_names_both_types(description: &str, expected: &str, got: &str) {
	let message = format!("expected {expected}, got {got}");
	assert!(description.contains(&message), "a sink write must fail with `{message}`, got: {description}");
	assert!(description.contains("CONSTRAINT_008"), "the failure must carry the digest write code: {description}");
}

#[test]
fn a_table_view_column_of_another_digest_or_a_scalar_type_fails_the_sink_write_naming_both_types() {
	// Without a sink check the digest reaches the row encoder, or fails as a cast the view never asked for (N17).
	let db = setup();
	for (view, column) in
		[("accuracy", "digest(float8, 0.05)"), ("inner", "digest(int4, 0.01)"), ("scalar", "Option(float8)")]
	{
		db.admin(&format!(
			"CREATE DEFERRED VIEW app::{view} {{ g: int4, d: {column} }} AS {{ FROM app::t | {DIGEST} by {{ g }} }}"
		));
	}
	insert_rows(&db);

	let description = failed_sink_writes(&db, 3);

	assert_names_both_types(&description, "Digest(Float8, 0.05)", "Digest(Float8, 0.01)");
	assert_names_both_types(&description, "Digest(Int4, 0.01)", "Digest(Float8, 0.01)");
	assert_names_both_types(&description, "Option(Float8)", "Digest(Float8, 0.01)");
	for view in ["accuracy", "inner", "scalar"] {
		assert_eq!(
			db.row_count(&format!("FROM app::{view}")),
			0,
			"a refused sink write must not leave a row in {view}"
		);
	}
}

#[test]
fn ringbuffer_and_series_view_columns_of_another_digest_fail_the_sink_write_naming_both_types() {
	// Ring buffer and series sinks encode rows on their own paths, so each must refuse the mismatch too.
	let db = setup();
	db.admin(&format!(
		"CREATE DEFERRED RINGBUFFER VIEW app::rb {{ g: int4, d: digest(float8, 0.02) }} WITH {{ capacity: 10 }} AS {{ FROM app::t | {DIGEST} by {{ g }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED SERIES VIEW app::sv {{ ts: int8, d: Option(digest(uint8, 0.01)) }} WITH {{ key: ts }} AS {{ FROM app::t | {DIGEST} by {{ ts }} }}"
	));
	insert_rows(&db);

	let description = failed_sink_writes(&db, 2);

	assert_names_both_types(&description, "Digest(Float8, 0.02)", "Digest(Float8, 0.01)");
	assert_names_both_types(&description, "Option(Digest(Uint8, 0.01))", "Digest(Float8, 0.01)");
	assert_eq!(
		db.row_count("FROM app::rb"),
		0,
		"a refused sink write must not leave a row in the ring buffer view"
	);
	assert_eq!(db.row_count("FROM app::sv"), 0, "a refused sink write must not leave a row in the series view");
}
