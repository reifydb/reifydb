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
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

fn poisoned_flows(db: &TestDb) -> String {
	let deadline = Instant::now() + TIMEOUT;
	loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status
		{
			return description.clone();
		}
		assert!(Instant::now() < deadline, "the view flow must be refused, last status: {status:?}");
		sleep(Duration::from_millis(20));
	}
}

#[test]
fn a_deferred_view_applying_an_unknown_operator_is_refused_naming_the_operator() {
	// An unknown operator must surface as an error naming it, never as a flow worker panic.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4 }");

	let created = db.try_admin("CREATE DEFERRED VIEW app::v { g: int4 } AS { FROM app::t | apply no_such_op {} }");
	if let Err(err) = created {
		let diagnostic = err.diagnostic();
		assert!(
			diagnostic.message.contains("no_such_op"),
			"a refused view must name the unknown operator, got: {diagnostic:?}"
		);
		return;
	}

	db.command("INSERT app::t [{ g: 1 }]");
	let description = poisoned_flows(&db);
	assert!(description.contains("no_such_op"), "the refused flow must name the unknown operator: {description}");
	assert!(
		description.contains("FLOW_026"),
		"the refused flow must carry the unknown operator code: {description}"
	);
	assert_eq!(db.row_count("FROM app::v"), 0, "a refused flow must not write a row to the view");
}
