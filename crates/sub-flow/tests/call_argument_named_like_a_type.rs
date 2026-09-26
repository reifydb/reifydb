// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::duration::Duration;

const TIMEOUT: Duration = Duration::from_seconds_const(5);

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn a_view_aggregate_over_a_column_named_like_a_type_reads_the_column() {
	// The flow compiles the same call as the ad hoc query, so a duration column must not turn into the duration
	// type here either.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::latency { g: int4, duration: int4 }");
	db.admin(
		"CREATE DEFERRED VIEW app::total { g: int4, total: int4 } AS { FROM app::latency | aggregate { total: math::sum(duration) } by { g } }",
	);

	db.command("INSERT app::latency [{ g: 1, duration: 5 }, { g: 1, duration: 7 }]");
	db.await_exact_row_count("FROM app::total", 1, TIMEOUT);

	let frames = db.query("FROM app::total");
	let totals: Vec<String> = column_values(&frames[0], "total").iter().map(|v| v.to_string()).collect();
	assert_eq!(totals, vec!["12"]);
}
