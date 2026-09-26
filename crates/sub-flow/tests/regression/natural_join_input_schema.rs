// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::thread::sleep;

use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_api::subsystem::HealthStatus;
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::{Value, duration::Duration};

const SETTLE: Duration = Duration::from_seconds_const(5);

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { a: int4, b: int4 }");
	db.admin("CREATE TABLE app::u { c: int4, e: int4 }");
	db.admin("CREATE TABLE app::w { a: int4, e: int4 }");
	db.admin("CREATE TABLE app::x { a: int4, b: int4, g: int4 }");
	db
}

fn insert_matching_rows(db: &TestDb) {
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command("INSERT app::u [{ c: 2, e: 200 }]");
	db.command("INSERT app::w [{ a: 2, e: 200 }]");
	db.command("INSERT app::x [{ a: 2, b: 99, g: 7 }]");
}

fn poisoned_flows(db: &TestDb) -> Result<String, String> {
	let deadline = Clock::Real.instant() + SETTLE;
	loop {
		let status =
			db.get_all_component_health().remove("flow").expect("the flow subsystem is registered").status;
		if let HealthStatus::Degraded {
			description,
		} = &status
		{
			return Ok(description.clone());
		}
		if Clock::Real.instant() >= deadline {
			return Err(format!("the view flow must be poisoned, last status: {status:?}"));
		}
		sleep(Duration::from_milliseconds_const(20).to_std());
	}
}

fn assert_view_fills_in(view: &str) -> Result<(), String> {
	let db = make_db();
	db.admin(view);
	insert_matching_rows(&db);
	match db.await_row_count("FROM app::v", 1, SETTLE) {
		1 => Ok(()),
		rows => Err(format!("the shared column a must join the two rows like a batch query, got {rows} rows")),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_column_renamed_by_map_fills_in() -> Result<(), String> {
	// Key names read from the map's parent schema miss the renamed column, so every row is dropped.
	assert_view_fills_in(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::u | map { a: c, e } } AS s }",
	)
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_map_that_drops_the_shared_column_is_an_error() -> Result<(), String> {
	// The map output has no a, so a view accepted on its parent's columns would stay empty forever.
	let db = make_db();
	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_x: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::w | map { x: a, e } } AS s }",
	);
	match created {
		Err(err) if err.0.code == "JOIN_002" => Ok(()),
		Err(err) => Err(format!("creating the view must report JOIN_002, got {err:?}")),
		Ok(_) => Err("creating the view must report JOIN_002, got Ok".to_string()),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_joined_right_side_fills_in() -> Result<(), String> {
	// A join without an output schema leaves the outer join no right columns to key on.
	assert_view_fills_in(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::w | INNER JOIN { FROM app::u } AS y USING (a, y.c) } AS s }",
	)
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_joined_left_side_fills_in() -> Result<(), String> {
	// A join without an output schema leaves the outer join no left columns to key on.
	assert_view_fills_in(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_e: int4 } AS { FROM app::t INNER JOIN { FROM app::u } AS y USING (a, y.c) NATURAL JOIN { FROM app::w } AS s }",
	)
}

#[test]
fn a_deferred_view_with_a_natural_join_on_an_aggregate_output_fills_in() -> Result<(), String> {
	// Key names read from the aggregate's parent schema include b, which the aggregate output lacks.
	assert_view_fills_in(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_n: int8 } AS { FROM app::t NATURAL JOIN { FROM app::x | aggregate { n: math::count(g) } by { a } } AS s }",
	)
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_rolling_window_output_fills_in() -> Result<(), String> {
	// Key names read from the window's parent schema include b, which the window output lacks.
	let db = make_db();
	db.admin("CREATE TABLE app::p { a: int4, b: int4, v: float8 } with { time: processing }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_total: float8 } AS { FROM app::t NATURAL JOIN { FROM app::p | window rolling { total: math::sum(v) } with { duration: 1h, lateness: 5m } by { a } } AS s }",
	);
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command("INSERT app::p [{ a: 2, b: 99, v: 1.5 }]");
	match db.await_row_count("FROM app::v", 1, SETTLE) {
		1 => Ok(()),
		rows => Err(format!("the shared column a must join the table row and the window row, got {rows} rows")),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_tumbling_window_output_fills_in() -> Result<(), String> {
	// Key names read from the window's parent schema include b, which the window output lacks.
	let db = make_db();
	db.admin("CREATE TABLE app::q { a: int4, b: int4, v: int4, ts: datetime } with { time: event(ts) }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_n: int8, s_opened: datetime } AS { FROM app::t NATURAL JOIN { FROM app::q | window tumbling { n: math::count(), opened: window::start() } by { a } with { duration: 60s, lateness: 0s } } AS s }",
	);
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command(r#"INSERT app::q [{ a: 2, b: 99, v: 7, ts: "2026-01-01T00:01:10Z" }]"#);
	match db.await_row_count("FROM app::v", 1, SETTLE) {
		1 => Ok(()),
		rows => Err(format!("the shared column a must join the table row and the window row, got {rows} rows")),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_sliding_window_output_fills_in() -> Result<(), String> {
	// Key names read from the window's parent schema include b, which the window output lacks.
	let db = make_db();
	db.admin("CREATE TABLE app::q { a: int4, b: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_total: float8 } AS { FROM app::t NATURAL JOIN { FROM app::q | window sliding { total: math::sum(v) } with { duration: 60s, slide: 15s, lateness: 0s } by { a } } AS s }",
	);
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command(r#"INSERT app::q [{ a: 2, b: 99, v: 1.5, ts: "2026-01-01T00:01:10Z" }]"#);
	match db.await_row_count("FROM app::v", 1, SETTLE) {
		0 => Err("the shared column a must join the table row and a sliding window row, got 0 rows".to_string()),
		_ => Ok(()),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_series_source_fills_in() -> Result<(), String> {
	// A series source with an empty output schema gives a natural join no key, so every row is dropped.
	let db = make_db();
	db.admin("CREATE SERIES app::s { ts: int8, a: int4, e: int4 } WITH { key: ts }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, r_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::s } AS r }",
	);
	db.command("INSERT app::t [{ a: 2, b: 20 }]");
	db.command("INSERT app::s [{ ts: 1, a: 2, e: 200 }]");
	match db.await_row_count("FROM app::v", 1, SETTLE) {
		1 => Ok(()),
		rows => Err(format!("the shared column a must join the table and series rows, got {rows} rows")),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_on_a_series_without_a_shared_column_is_an_error() -> Result<(), String> {
	// A series side the create check cannot see through leaves a view that stays empty forever.
	let db = make_db();
	db.admin("CREATE SERIES app::s { ts: int8, e: int4 } WITH { key: ts }");
	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, r_ts: int8, r_e: int4 } AS { FROM app::t NATURAL JOIN { FROM app::s } AS r }",
	);
	match created {
		Err(err) if err.0.code == "JOIN_002" => Ok(()),
		Err(err) => Err(format!("creating the view must report JOIN_002, got {err:?}")),
		Ok(_) => Err("creating the view must report JOIN_002, got Ok".to_string()),
	}
}

#[test]
fn a_deferred_view_with_a_natural_join_whose_joined_side_shares_no_column_reports_join_002() -> Result<(), String> {
	// A join side the view compiler cannot see through must still fail loud, never hold 0 rows silently.
	let db = make_db();
	let created = db.try_admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4 } AS { FROM app::t NATURAL JOIN { FROM app::u | INNER JOIN { FROM app::w } AS y USING (c, y.a) } AS s }",
	);
	if let Err(err) = created {
		return match err.0.code.as_str() {
			"JOIN_002" => Ok(()),
			_ => Err(format!("creating the view must report JOIN_002, got {err:?}")),
		};
	}

	insert_matching_rows(&db);
	let description = poisoned_flows(&db)?;
	if !description.contains("JOIN_002") {
		return Err(format!("the refused flow must carry JOIN_002: {description}"));
	}
	match db.row_count("FROM app::v") {
		0 => Ok(()),
		rows => Err(format!("a refused flow must not write a row to the view, got {rows} rows")),
	}
}

#[test]
fn a_deferred_view_with_a_left_join_on_an_extended_side_writes_none_for_an_unmatched_row() -> Result<(), String> {
	// Extended right columns carry no known type, yet an unmatched row must still write none for them.
	let db = make_db();
	db.admin(
		"CREATE DEFERRED VIEW app::v { a: int4, b: int4, s_k: Option(int4), s_z: Option(int4) } AS { FROM app::t LEFT JOIN { FROM app::u | extend { k: c, z: e + 1 } } AS s USING (a, s.k) }",
	);
	db.command("INSERT app::t [{ a: 5, b: 50 }]");

	let rows = db.await_row_count("FROM app::v", 1, SETTLE);
	if rows != 1 {
		return Err(format!("the unmatched left row must reach the view, got {rows} rows"));
	}
	let frames = db.query("FROM app::v");
	let s_k = column_values(&frames[0], "s_k");
	let s_z = column_values(&frames[0], "s_z");
	let holds_one_none = |values: &[Value]| matches!(values, [Value::None { .. }]);
	match holds_one_none(&s_k) && holds_one_none(&s_z) {
		true => Ok(()),
		false => Err(format!(
			"the unmatched row must hold none for s_k and s_z, got s_k {s_k:?} and s_z {s_z:?}"
		)),
	}
}
