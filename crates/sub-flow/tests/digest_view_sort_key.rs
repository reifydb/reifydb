// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_sub_api::subsystem::HealthStatus;

const TIMEOUT: Duration = Duration::from_secs(10);

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, latency: float8 }");
	db
}

fn poisoned_flows(db: &TestDb, expected: usize) -> String {
	let deadline = Instant::now() + TIMEOUT;
	loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status
			&& description.starts_with(&format!("{expected} deferred flow(s) poisoned"))
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

#[test]
fn a_view_sorted_by_a_digest_column_fails_its_sink_write_instead_of_panicking() {
	// A digest has no key encoding, so the sort key must fail the flow with SERDE_003, never panic the worker.
	let db = setup();
	db.admin("CREATE DEFERRED VIEW app::sorted { g: int4, d: digest(float8, 0.01) } \
		 AS { FROM app::t | aggregate { d: stats::digest(latency, 0.01) } by { g } | sort { d } }");
	db.admin("CREATE DEFERRED VIEW app::partitioned { g: int4, d: digest(float8, 0.01) } \
		 WITH { partition: { by: { g } } } \
		 AS { FROM app::t | aggregate { d: stats::digest(latency, 0.01) } by { g } | sort { d } }");
	db.command("INSERT app::t [{ g: 1, latency: 1.5 }, { g: 2, latency: 3.0 }]");

	let description = poisoned_flows(&db, 2);

	assert_eq!(
		description.matches("SERDE_003").count(),
		2,
		"both sorted views must fail on the digest sort key: {description}"
	);
	assert_eq!(db.row_count("FROM app::sorted"), 0, "a refused sort key must not leave a row in the view");
	assert_eq!(db.row_count("FROM app::partitioned"), 0, "a refused sort key must not leave a row in the view");
}
