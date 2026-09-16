// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_sub_api::subsystem::HealthStatus;

const SETTLE: Duration = Duration::from_secs(5);

const PICK_ON_A_MISSING_COLUMN: &str = "CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t INNER JOIN { FROM app::w } AS s USING (a, s.a) WITH { snapshot: true, latest: { no_such } } }";

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { a: int4, b: int4 }");
	db.admin("CREATE TABLE app::w { a: int4, e: int4 }");
	db
}

fn insert_matching_rows(db: &TestDb) {
	db.command("INSERT app::w [{ a: 2, e: 200 }]");
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
}

#[test]
fn a_join_pick_on_a_column_an_aggregate_right_side_lacks_poisons_the_flow_naming_it() {
	// An aggregate right side has no column list at create, so the step must fail with an error, never panic.
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_n: int8 } AS { FROM app::t INNER JOIN { FROM app::w | aggregate { n: math::count(e) } by { a } } AS s USING (a, s.a) WITH { snapshot: true, latest: { no_such } } }",
	);
	insert_matching_rows(&db);

	let deadline = Instant::now() + SETTLE;
	let status = loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if matches!(status, HealthStatus::Degraded { .. }) || Instant::now() >= deadline {
			break status;
		}
		sleep(Duration::from_millis(20));
	};

	let HealthStatus::Degraded {
		description,
	} = &status
	else {
		panic!("the pick on no_such must poison the flow with an error, got {status:?}");
	};
	assert!(
		description.contains("JOIN_003") && description.contains("no_such"),
		"the poisoned flow must name the pick column, got: {description}"
	);
}

#[test]
fn a_join_pick_on_a_column_the_right_side_lacks_is_refused_at_create() {
	// The right side's columns are known at create, so an unknown pick column must never panic a flow step.
	let db = make_db();
	match db.try_admin(PICK_ON_A_MISSING_COLUMN) {
		Err(err) => {
			let rendered = format!("{err:?}");
			assert!(rendered.contains("no_such"), "the refusal must name the pick column, got {rendered}");
		}
		Ok(_) => panic!("latest orders by no_such, which app::w does not have, so creating the view must fail"),
	}
}
