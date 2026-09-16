// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_sub_api::subsystem::HealthStatus;

const SETTLE: Duration = Duration::from_secs(5);

const TWICE: &str = "UDF twice ($x: int4): int4 { RETURN $x * 2 }";

fn setup() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, a: int4 }");
	db.admin("CREATE TABLE app::u { g: int4, c: int4 }");
	db.admin("CREATE TABLE app::e { g: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db
}

fn refusal_of(view: &str) -> Result<(), String> {
	let db = setup();
	let created = db.try_admin(&format!("{TWICE}; {view}"));
	let Err(err) = created else {
		return Err("creating the view must fail, got Ok".to_string());
	};
	let diagnostic = err.diagnostic();
	if diagnostic.code != "FLOW_061" {
		return Err(format!("the refusal must be FLOW_061, got {diagnostic:?}"));
	}
	if !diagnostic.message.contains("twice") || diagnostic.fragment.text() != "twice" {
		return Err(format!("the refusal must name twice and point at the call, got {diagnostic:?}"));
	}
	Ok(())
}

#[test]
fn a_deferred_view_calling_a_script_udf_is_refused_in_every_expression_position() {
	// A position the create check skips reaches the flow without the script's udfs and aborts the flow.
	let positions = [
		(
			"extend",
			"CREATE DEFERRED VIEW app::v { g: int4, a: int4, x: int4 } AS { FROM app::t | extend { x: twice(a) } }",
		),
		(
			"filter",
			"CREATE DEFERRED VIEW app::v { g: int4, a: int4 } AS { FROM app::t | filter { twice(a) > 2 } }",
		),
		(
			"nested in map",
			"CREATE DEFERRED VIEW app::v { g: int4, x: int4 } AS { FROM app::t | map { g, x: twice(a) + 1 } }",
		),
		(
			"aggregate input",
			"CREATE DEFERRED VIEW app::v { g: int4, s: int8 } AS { FROM app::t | aggregate { s: math::sum(twice(a)) } by { g } }",
		),
		(
			"aggregate by",
			"CREATE DEFERRED VIEW app::v { g: int4, s: int8 } AS { FROM app::t | aggregate { s: math::sum(a) } by { twice(g) } }",
		),
		(
			"window aggregation",
			"CREATE DEFERRED VIEW app::v { g: int4, total: int8 } AS { FROM app::e | window tumbling { total: math::sum(twice(v)) } with { duration: 1s, lateness: 0s } by { g } }",
		),
		(
			"joined side",
			"CREATE DEFERRED VIEW app::v { g: int4, a: int4, s_g: int4, s_x: int4 } AS { FROM app::t INNER JOIN { FROM app::u | map { g, x: twice(c) } } AS s USING (g, s.g) }",
		),
		(
			"appended side",
			"CREATE DEFERRED VIEW app::v { g: int4, a: int4 } AS { FROM app::t APPEND { FROM app::u | map { g, a: twice(c) } } }",
		),
	];
	let misses: Vec<String> = positions
		.iter()
		.filter_map(|(position, view)| refusal_of(view).err().map(|miss| format!("{position}: {miss}")))
		.collect();
	assert!(misses.is_empty(), "every position calling twice must be refused at create:\n{}", misses.join("\n"));
}

#[test]
fn a_deferred_view_calling_a_builtin_function_is_accepted_and_fills_in() {
	// The udf refusal must never catch a builtin call, which the flow can run.
	let db = setup();
	db.admin(&format!(
		"{TWICE}; CREATE DEFERRED VIEW app::v {{ g: int4, x: int4 }} AS {{ FROM app::t | map {{ g, x: math::abs(a) }} }}"
	));
	db.command("INSERT app::t [{ g: 1, a: -4 }]");
	assert_eq!(db.await_row_count("FROM app::v", 1, SETTLE), 1, "the builtin call must fill the view");
}

fn unknown_function_fails_loud(view: &str) {
	let db = setup();
	if let Err(err) = db.try_admin(view) {
		let rendered = format!("{err:?}");
		assert!(rendered.contains("no_such_fn"), "the refusal must name no_such_fn, got {rendered}");
		return;
	}

	db.command("INSERT app::t [{ g: 1, a: 2 }]");
	let deadline = Instant::now() + SETTLE;
	let description = loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status
		{
			break description.clone();
		}
		assert!(Instant::now() < deadline, "the view flow must be poisoned, last status: {status:?}");
		sleep(Duration::from_millis(20));
	};
	assert!(
		description.contains("FUNCTION_001") && description.contains("no_such_fn"),
		"the poisoned flow must carry FUNCTION_001 naming no_such_fn: {description}"
	);
	assert_eq!(db.row_count("FROM app::v"), 0, "a poisoned flow must not write a row to the view");
}

#[test]
fn a_deferred_view_mapping_an_unknown_function_fails_loud_without_a_panic() {
	// An unknown name in map must end as a named error, never as a panic that takes down the flow worker.
	unknown_function_fails_loud(
		"CREATE DEFERRED VIEW app::v { g: int4, x: int4 } AS { FROM app::t | map { g, x: no_such_fn(a) } }",
	);
}

#[test]
fn a_deferred_view_extending_with_an_unknown_function_fails_loud_without_a_panic() {
	// An unknown name in extend must end as a named error, never as a panic that takes down the flow worker.
	unknown_function_fails_loud(
		"CREATE DEFERRED VIEW app::v { g: int4, a: int4, x: int4 } AS { FROM app::t | extend { x: no_such_fn(a) } }",
	);
}
