// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_test_harness::assert::rows;
use reifydb_value::value::{Value, digest::Digest, duration::Duration, value_type::ValueType};

const TIMEOUT: Duration = Duration::from_seconds_const(30);
const PPM: u32 = 10_000;
const P50: &str = "p50: stats::approx_percentile(v, 0.5, 0.01)";

fn memory() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

fn bucketed(value: f64) -> Value {
	let mut one = Digest::new(ValueType::Float8, PPM).expect("the accuracy is in range");
	one.add_value(&Value::float8(value)).expect("a float8 is a digest input");
	one.percentile_value(1.0).expect("a one-value digest has a percentile")
}

fn median(values: &[f64]) -> Value {
	let mut sorted = values.to_vec();
	sorted.sort_by(f64::total_cmp);
	bucketed(sorted[sorted.len().div_ceil(2) - 1])
}

fn declare(db: &TestDb, views: &[(&str, &str, &str, &str)]) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	for (name, kind, calls, with) in views {
		let columns = if calls.contains("lo:") {
			"p50: Option(float8), lo: Option(float8)"
		} else {
			"p50: Option(float8)"
		};
		db.admin(&format!(
			"CREATE DEFERRED VIEW app::{name} {{ g: int4, {columns} }} AS {{ FROM app::t | window {kind} {{ {calls} }} with {{ {with} }} by {{ g }} }}"
		));
	}
}

fn at(minute: u64) -> String {
	format!("2026-01-01T12:{minute:02}:00Z")
}

fn insert(db: &TestDb, rows: &[(i32, i32, u64, f64)]) {
	let literals: Vec<String> = rows
		.iter()
		.map(|(id, g, minute, v)| format!(r#"{{ id: {id}, g: {g}, v: {v:?}, ts: "{}" }}"#, at(*minute)))
		.collect();
	db.command(&format!("INSERT app::t [{}]", literals.join(", ")));
}

fn published(db: &TestDb, view: &str, columns: &[&str]) -> Vec<Vec<Value>> {
	let mut out: Vec<Vec<Value>> = rows(&db.query(&format!("FROM app::{view}")))
		.into_iter()
		.map(|row| {
			let cell = |name: &str| {
				row.iter()
					.find(|(column, _)| column == name)
					.map(|(_, value)| value.clone())
					.unwrap_or_else(|| panic!("app::{view} row has no column {name}: {row:?}"))
			};
			let mut cells = vec![cell("g")];
			cells.extend(columns.iter().map(|name| cell(name)));
			cells
		})
		.collect();
	out.sort_by_key(|row| format!("{row:?}"));
	out
}

fn assert_view(db: &TestDb, view: &str, columns: &[&str], mut want: Vec<Vec<Value>>, step: &str) {
	want.sort_by_key(|row| format!("{row:?}"));
	assert!(db.await_all_flows(TIMEOUT), "{step}: the flows must process every change");
	let got = await_value(want.clone(), TIMEOUT, || published(db, view, columns));
	assert_eq!(got, want, "{step}: app::{view}");
}

fn medians_of(windows: BTreeMap<(i32, u64), Vec<f64>>) -> Vec<Vec<Value>> {
	windows.into_iter().map(|((g, _), values)| vec![Value::Int4(g), median(&values)]).collect()
}

fn sliding_windows(table: &BTreeMap<i32, (i32, u64, f64)>) -> Vec<Vec<Value>> {
	let mut windows: BTreeMap<(i32, u64), Vec<f64>> = BTreeMap::new();
	for (g, minute, v) in table.values() {
		for start in (0..=*minute).step_by(5).filter(|start| *minute < start + 10) {
			windows.entry((*g, start)).or_default().push(*v);
		}
	}
	medians_of(windows)
}

#[test]
fn a_sliding_percentile_view_equals_the_median_of_each_window_through_inserts_deletes_and_updates() {
	// Every row sits in two windows, so a delete or update that reaches only one leaves the other digest wrong.
	let db = memory();
	declare(&db, &[("s", "sliding", P50, "duration: 10m, slide: 5m, lateness: 1h")]);
	let mut table: BTreeMap<i32, (i32, u64, f64)> = BTreeMap::new();

	for chunk in (1..=13).collect::<Vec<i32>>().chunks(3) {
		let rows: Vec<(i32, i32, u64, f64)> =
			chunk.iter().map(|id| (*id, 1 + id % 2, *id as u64 + 5, ((id * 37) % 97 + 1) as f64)).collect();
		insert(&db, &rows);
		table.extend(rows.iter().map(|(id, g, minute, v)| (*id, (*g, *minute, *v))));
		assert_view(&db, "s", &["p50"], sliding_windows(&table), &format!("insert rows {chunk:?}"));
	}

	db.command("DELETE app::t FILTER { id == 3 or id == 8 }");
	table.remove(&3);
	table.remove(&8);
	assert_view(&db, "s", &["p50"], sliding_windows(&table), "delete rows in two windows each");

	db.command("UPDATE app::t { v: 500.0 } FILTER { id == 5 }");
	db.command("UPDATE app::t { v: 0.5 } FILTER { id == 6 }");
	table.get_mut(&5).expect("row 5 is live").2 = 500.0;
	table.get_mut(&6).expect("row 6 is live").2 = 0.5;
	assert_view(&db, "s", &["p50"], sliding_windows(&table), "update rows in two windows each");

	db.command("DELETE app::t FILTER { id >= 10 }");
	table.retain(|id, _| *id < 10);
	assert_view(&db, "s", &["p50"], sliding_windows(&table), "delete every row of the newest windows");
}

#[test]
fn a_session_percentile_view_equals_the_median_of_each_session_through_inserts_deletes_and_updates() {
	// A delete must not move a session boundary, so a digest that follows the rows but not the sessions goes wrong.
	let db = memory();
	declare(&db, &[("s", "session", P50, "gap: 2m, lateness: 1h")]);
	let arrivals: [(i32, i32, u64, f64, u64); 9] = [
		(1, 1, 0, 30.0, 0),
		(2, 2, 0, 4.0, 0),
		(3, 1, 1, 10.0, 0),
		(4, 1, 2, 20.0, 0),
		(5, 1, 3, 90.0, 0),
		(6, 2, 5, 8.0, 1),
		(7, 1, 10, 70.0, 1),
		(8, 2, 6, 2.0, 1),
		(9, 1, 11, 60.0, 1),
	];
	let mut table: BTreeMap<i32, (i32, u64, f64)> = BTreeMap::new();
	let sessions = |table: &BTreeMap<i32, (i32, u64, f64)>| {
		let mut windows: BTreeMap<(i32, u64), Vec<f64>> = BTreeMap::new();
		for (g, session, v) in table.values() {
			windows.entry((*g, *session)).or_default().push(*v);
		}
		medians_of(windows)
	};
	for (id, g, minute, v, session) in arrivals {
		insert(&db, &[(id, g, minute, v)]);
		table.insert(id, (g, session, v));
	}
	assert_view(&db, "s", &["p50"], sessions(&table), "two sessions per group");

	db.command("DELETE app::t FILTER { id == 3 }");
	table.remove(&3);
	assert_view(&db, "s", &["p50"], sessions(&table), "delete a row inside the first session");

	db.command("UPDATE app::t { v: 1.0 } FILTER { id == 9 }");
	db.command("UPDATE app::t { v: 50.0 } FILTER { id == 2 }");
	table.get_mut(&9).expect("row 9 is live").2 = 1.0;
	table.get_mut(&2).expect("row 2 is live").2 = 50.0;
	assert_view(&db, "s", &["p50"], sessions(&table), "update a row in each group");

	db.command("DELETE app::t FILTER { id == 6 or id == 8 }");
	table.remove(&6);
	table.remove(&8);
	assert_view(&db, "s", &["p50"], sessions(&table), "delete every row of a session");
}

#[test]
fn a_retraction_older_than_immutable_leaves_the_percentile_but_not_the_sealed_min_in_tumbling_sliding_and_session_views()
 {
	// D34: the digest applies every retraction, while a min sealed past immutable keeps the retracted value.
	let db = memory();
	let calls = "p50: stats::approx_percentile(v, 0.5, 0.01), lo: math::min(v)";
	let views = [
		("tumbling", "tumbling", calls, "duration: 1h, lateness: 1h, immutable: 5m", 1),
		("sliding", "sliding", calls, "duration: 1h, slide: 30m, lateness: 1h, immutable: 5m", 2),
		("session", "session", calls, "gap: 30m, lateness: 1h, immutable: 5m", 1),
	];
	declare(&db, &views.map(|(name, kind, calls, with, _)| (name, kind, calls, with)));
	let expect = |p50: Value, lo: f64, windows: usize| vec![vec![Value::Int4(1), p50, Value::float8(lo)]; windows];

	insert(&db, &[(1, 1, 0, 1.0), (2, 1, 1, 50.0)]);
	insert(&db, &[(3, 1, 10, 20.0), (4, 1, 10, 30.0)]);
	for (name, _, _, _, windows) in views {
		let want = expect(median(&[1.0, 50.0, 20.0, 30.0]), 1.0, windows);
		assert_view(&db, name, &["p50", "lo"], want, "the 12:00 and 12:01 rows sealed by the 12:10 rows");
	}

	db.command("DELETE app::t FILTER { id == 1 }");
	for (name, _, _, _, windows) in views {
		let want = expect(median(&[50.0, 20.0, 30.0]), 1.0, windows);
		assert_view(&db, name, &["p50", "lo"], want, "a delete ten minutes older than the newest row");
	}

	db.command("DELETE app::t FILTER { id >= 2 }");
	for (name, _, _, _, windows) in views {
		let want = expect(Value::none_of(ValueType::Float8), 1.0, windows);
		assert_view(&db, name, &["p50", "lo"], want, "every row deleted behind a sealed min");
	}
}
