// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The rolling operator carries its accumulators and meta caches across batches. Each INSERT below
// is its own commit, so a wrong total means the state carried over disagrees with what was
// committed - a double-merge, a stale high_water, or a missed eviction.

use reifydb::{
	WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_test_harness::assert::rows;
use reifydb_value::value::{Value, digest::Digest, duration::Duration, value_type::ValueType};

const TIMEOUT: Duration = Duration::from_seconds_const(5);

const PPM: u32 = 10_000;

fn setup() -> TestDb {
	TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"))
}

#[test]
fn rolling_sum_accumulates_correctly_across_separate_commits() {
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	let insert = |g: i32, v: f64, ts: &str| {
		db.command(&format!("INSERT app::t [{{ g: {g}, v: {v}, ts: \"{ts}\" }}]"));
	};
	let await_total = |g: i32, total: f64| {
		let rql = format!("FROM app::r | filter {{ g == {g} and total == {total} }}");
		let got = db.await_row_count(&rql, 1, Duration::from_seconds_const(5));
		assert_eq!(
			got,
			1,
			"group {g} must roll up to {total} from state carried across commits; view now: {:?}",
			db.query_as_root("FROM app::r", ())
		);
	};

	// Batch 1 creates the group's running state from scratch.
	insert(1, 10.0, "2026-01-01T00:00:00Z");
	await_total(1, 10.0);

	// Batch 2 must fold into the state left behind by batch 1, not restart from zero
	// (missed carry-over) and not count batch 1 twice (double-merge on reload).
	insert(1, 5.0, "2026-01-01T00:01:00Z");
	await_total(1, 15.0);

	// An unrelated group gets its own accumulator without disturbing group 1.
	insert(2, 7.0, "2026-01-01T00:01:00Z");
	await_total(2, 7.0);
	await_total(1, 15.0);

	// A third batch for group 1 keeps compounding on the twice-carried state.
	insert(1, 3.0, "2026-01-01T00:02:00Z");
	await_total(1, 18.0);

	// One materialized row per group: updates must rewrite the group's row, not append.
	let rows = db.row_count("FROM app::r");
	assert_eq!(rows, 2, "rolling view must hold exactly one row per group, got {rows}");
}

#[test]
fn a_processing_domain_rolling_window_rolls_up_over_the_rows_own_times() {
	// A time-based rolling window takes the instant-keyed engine in either domain; the row-numbered
	// one is reserved for count-based windows. This is the operator's only processing-domain
	// coverage, so a carry-over or double-merge fault there would otherwise surface in production.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8 } with { time: processing }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	let insert = |g: i32, v: f64| {
		db.command(&format!("INSERT app::t [{{ g: {g}, v: {v} }}]"));
	};
	let await_total = |g: i32, total: f64| {
		let rql = format!("FROM app::p | filter {{ g == {g} and total == {total} }}");
		let got = db.await_row_count(&rql, 1, Duration::from_seconds_const(5));
		assert_eq!(
			got,
			1,
			"group {g} must roll up to {total} on the unified engine; view now: {:?}",
			db.query_as_root("FROM app::p", ())
		);
	};

	insert(1, 10.0);
	await_total(1, 10.0);

	// The second batch must fold into the first's state rather than restart from zero.
	insert(1, 5.0);
	await_total(1, 15.0);

	// A second group must get its own accumulator without disturbing the first.
	insert(2, 7.0);
	await_total(2, 7.0);
	await_total(1, 15.0);

	// Several rows in one commit must count three times, not collapse onto a shared coordinate.
	db.command("INSERT app::t [{ g: 1, v: 1.0 }, { g: 1, v: 2.0 }, { g: 1, v: 3.0 }]");
	await_total(1, 21.0);

	let rows = db.row_count("FROM app::p");
	assert_eq!(rows, 2, "rolling view must hold exactly one row per group, got {rows}");
}

#[test]
fn an_event_view_over_an_event_source_buckets_by_the_declared_column_not_by_arrival() {
	// Both rows share one arrival but their ts values are 5h apart, so only a coordinate taken
	// from ts ages one out - bucketing by arrival would keep both and total 9.0. This is the whole
	// event-time contract in one assertion.
	//
	// Its former control - a view declaring `time: processing` over this same source, asserting it
	// bucketed by arrival instead - was deleted with the flow-level time declaration. Worth
	// recording why: once views stopped declaring, that control became a byte-identical copy of
	// this view while its name and comments still claimed it re-timed to arrival, so it passed
	// while asserting something false. A green test that cannot fail is worse than none.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::e { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	db.command(
		r#"INSERT app::t [
			{ g: 1, v: 4.0, ts: "2020-03-01T00:00:00Z" },
			{ g: 1, v: 5.0, ts: "2020-03-01T05:00:00Z" }
		]"#,
	);

	let settled = db.await_row_count("FROM app::e | filter { total == 5.0 }", 1, Duration::from_seconds_const(5));
	assert_eq!(
		settled,
		1,
		"the 00:00 row sits more than 1h+5m behind the 05:00 row's event time and must age out of \
		 the rolling span, leaving 5.0; a total of 9.0 means both rows were bucketed by arrival \
		 instead of by ts. view now: {:?}",
		db.query_as_root("FROM app::e", ())
	);
}

#[test]
fn a_processing_view_over_a_processing_source_keeps_its_rows_live() {
	// The source declares no ts, so its #time is arrival, and the window buckets on that same
	// stamp. This is now the whole contract for a processing-time source: it is legal, it creates
	// without a declaration, and coordinate and watermark agree because both read #time. A
	// companion test that asserted an event-time view over such a source was REFUSED was deleted
	// with the flow-level time declaration - there is no longer a second clock to disagree with,
	// so there is nothing left to refuse. The one fault this shape can still hide is stamping
	// epoch instead of arrival, putting every row ~56 years behind the cutoff - hence the
	// assertion that the rows stay live.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, v: float8 } with { time: processing }");
	db.admin(r#"CREATE DEFERRED VIEW app::p { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	db.command("INSERT app::t [{ g: 1, v: 6.0 }]");
	assert_eq!(
		db.await_row_count("FROM app::p | filter { total == 6.0 }", 1, Duration::from_seconds_const(5)),
		1,
		"view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);

	let held =
		db.await_exact_row_count("FROM app::p | filter { total == 6.0 }", 1, Duration::from_seconds_const(2));
	assert_eq!(
		held,
		1,
		"a row stamped with its arrival must stay inside a 1h rolling window; draining to zero means \
		 the stamp was epoch, not arrival. view now: {:?}",
		db.query_as_root("FROM app::p", ())
	);
}

#[test]
fn a_row_too_late_to_admit_does_not_delete_the_group_it_belongs_to() {
	// A late row must be ignored, not withdraw the whole group it belongs to. Lateness is decided
	// by the seal ledger, which moves only when a seal timer fires, so the 14:00 row is not
	// padding: without it nothing can be late and the refusal path is never entered.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 10.0, ts: "2026-01-01T12:00:00Z" }]"#);
	db.await_row_count("FROM app::r | filter { g == 1 and total == 10.0 }", 1, TIMEOUT);

	// Carries the watermark past the 13:00 seal timer, which is what finally advances the ledger.
	// It also rolls the 12:00 contribution out of the 1h window, leaving group 1 on 20.
	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 20.0, ts: "2026-01-01T14:00:00Z" }]"#);
	db.await_row_count("FROM app::r | filter { g == 1 and total == 20.0 }", 1, TIMEOUT);

	// Five hours behind the ledger, so past interval + lateness and refused as late. It is batched
	// with an admitted row for another group so the batch cannot early-return on empty buckets.
	db.command(
		r#"INSERT app::t [{ id: 3, g: 1, v: 99.0, ts: "2026-01-01T09:00:00Z" }, { id: 4, g: 2, v: 1.0, ts: "2026-01-01T14:00:00Z" }]"#,
	);
	db.await_all_flows(TIMEOUT);

	let held = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 20.0 }", 1, TIMEOUT);
	assert_eq!(
		held,
		1,
		"a refused row must leave the group's rolling total untouched, not withdraw it; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
}

#[test]
fn retracting_a_row_that_has_already_left_the_window_leaves_the_group_intact() {
	// The same "no result means gone" confusion through the other door: the lateness is wider than the
	// interval, so a coordinate can be new enough to admit while already older than the trailing
	// window. Retracting one changes nothing, and that silence must not withdraw the group.
	let db = setup();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(r#"CREATE DEFERRED VIEW app::r { g: int4, total: float8 } AS {
			FROM app::t
				| window rolling { total: math::sum(v) }
					with { duration: 1h, lateness: 5m }
					by { g }
		}"#);

	db.command(r#"INSERT app::t [{ id: 1, g: 1, v: 4.0, ts: "2026-01-01T11:30:00Z" }]"#);
	db.await_row_count("FROM app::r | filter { g == 1 and total == 4.0 }", 1, TIMEOUT);

	db.command(r#"INSERT app::t [{ id: 2, g: 1, v: 6.0, ts: "2026-01-01T13:00:00Z" }]"#);
	let rolled = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 6.0 }", 1, TIMEOUT);
	assert_eq!(
		rolled,
		1,
		"the 11:30 contribution must fall out of a 1h window ending at 13:00, leaving 6.0; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);

	// 11:30 is still inside interval + lateness of the 13:00 watermark, so this delete is admitted
	// and routed - but it targets a coordinate the window no longer holds.
	db.command("DELETE app::t FILTER { id == 1 }");
	db.await_all_flows(TIMEOUT);

	let intact = db.await_exact_row_count("FROM app::r | filter { g == 1 and total == 6.0 }", 1, TIMEOUT);
	assert_eq!(
		intact,
		1,
		"retracting a contribution that already left the window is a no-op, so the group must keep \
		 its total rather than be withdrawn; view now: {:?}",
		db.query_as_root("FROM app::r", ())
	);
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

fn group_rows(db: &TestDb, view: &str, g: i32, columns: &[&str]) -> Vec<Vec<Value>> {
	rows(&db.query(&format!("FROM {view} | filter {{ g == {g} }}")))
		.into_iter()
		.map(|row| {
			columns.iter()
				.map(|name| {
					row.iter()
						.find(|(column, _)| column == name)
						.map(|(_, value)| value.clone())
						.unwrap_or_else(|| panic!("{view} row has no column {name}: {row:?}"))
				})
				.collect()
		})
		.collect()
}

fn await_group(db: &TestDb, view: &str, g: i32, columns: &[&str], want: Vec<Vec<Value>>, step: &str) {
	let got = await_value(want.clone(), TIMEOUT, || group_rows(db, view, g, columns));
	assert_eq!(
		got,
		want,
		"{step}: group {g} of {view}; view now: {:?}",
		db.query_as_root(&format!("FROM {view}"), ())
	);
}

fn percentile_view(db: &TestDb, calls: &str, columns: &str, with: &str) {
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, v: float8, ts: datetime } with { time: event(ts) }");
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::r {{ g: int4, {columns} }} AS {{ FROM app::t | window rolling {{ {calls} }} with {{ {with} }} by {{ g }} }}"
	));
}

fn insert(db: &TestDb, rows: &[(i32, i32, f64, &str)]) {
	let literals: Vec<String> = rows
		.iter()
		.map(|(id, g, v, at)| format!(r#"{{ id: {id}, g: {g}, v: {v:?}, ts: "2026-01-01T{at}Z" }}"#))
		.collect();
	db.command(&format!("INSERT app::t [{}]", literals.join(", ")));
}

#[test]
fn a_rolling_percentile_across_separate_commits_reads_one_digest_over_every_row_in_the_frame() {
	// A median of per-slot medians differs from the frame median on these rows, so a slot fold must fail.
	let db = setup();
	percentile_view(
		&db,
		"p50: stats::approx_percentile(v, 0.5, 0.01)",
		"p50: Option(float8)",
		"duration: 1h, lateness: 5m",
	);

	let slots = [("12:00:00", [1.0, 2.0, 3.0]), ("12:01:00", [40.0, 50.0, 60.0]), ("12:02:00", [7.0, 80.0, 90.0])];
	let mut frame = Vec::new();
	let mut id = 0;
	for (at, values) in slots {
		for v in values {
			id += 1;
			insert(&db, &[(id, 1, v, at)]);
			frame.push(v);
		}
		await_group(
			&db,
			"app::r",
			1,
			&["p50"],
			vec![vec![median(&frame)]],
			&format!("commits into the {at} slot"),
		);
	}
	assert_ne!(
		median(&frame),
		median(&[2.0, 50.0, 80.0]),
		"the frame median must differ from the slot medians' median"
	);

	insert(&db, &[(10, 1, 5.0, "13:01:30")]);
	await_group(
		&db,
		"app::r",
		1,
		&["p50"],
		vec![vec![median(&[7.0, 80.0, 90.0, 5.0])]],
		"a row an hour on evicts the 12:00 and 12:01 slots",
	);
}

#[test]
fn a_row_too_late_to_admit_leaves_the_rolling_percentile_intact() {
	// A late 1.0 taken into the digest moves the median from 30 to 20, and a withdrawn group reads nothing.
	let db = setup();
	percentile_view(
		&db,
		"p50: stats::approx_percentile(v, 0.5, 0.01)",
		"p50: Option(float8)",
		"duration: 1h, lateness: 5m",
	);

	insert(&db, &[(1, 1, 10.0, "12:00:00")]);
	await_group(&db, "app::r", 1, &["p50"], vec![vec![bucketed(10.0)]], "the first row");

	insert(&db, &[(2, 1, 20.0, "14:00:00"), (3, 1, 30.0, "14:00:00"), (4, 1, 40.0, "14:00:00")]);
	await_group(&db, "app::r", 1, &["p50"], vec![vec![median(&[20.0, 30.0, 40.0])]], "the 12:00 row rolled out");
	assert_ne!(median(&[20.0, 30.0, 40.0]), median(&[1.0, 20.0, 30.0, 40.0]), "the late row must be observable");

	insert(&db, &[(5, 1, 1.0, "09:00:00"), (6, 2, 1.0, "14:00:00")]);
	assert!(db.await_all_flows(TIMEOUT), "the flows must process the late row");
	await_group(&db, "app::r", 2, &["p50"], vec![vec![bucketed(1.0)]], "the row batched with the late one");
	await_group(&db, "app::r", 1, &["p50"], vec![vec![median(&[20.0, 30.0, 40.0])]], "a refused late row");
}

#[test]
fn retracting_a_row_that_already_left_the_frame_leaves_the_rolling_percentile_intact() {
	// Taking an evicted value out of the digest removes a value the frame does not hold and moves the median.
	let db = setup();
	percentile_view(
		&db,
		"p50: stats::approx_percentile(v, 0.5, 0.01)",
		"p50: Option(float8)",
		"duration: 1h, lateness: 5m",
	);

	insert(&db, &[(1, 1, 1.0, "11:30:00")]);
	await_group(&db, "app::r", 1, &["p50"], vec![vec![bucketed(1.0)]], "the first row");

	insert(&db, &[(2, 1, 6.0, "13:00:00"), (3, 1, 8.0, "13:00:00"), (4, 1, 9.0, "13:00:00")]);
	await_group(&db, "app::r", 1, &["p50"], vec![vec![median(&[6.0, 8.0, 9.0])]], "the 11:30 row rolled out");
	assert_ne!(median(&[6.0, 8.0, 9.0]), median(&[1.0, 6.0, 8.0, 9.0]), "the eviction must be observable");

	db.command("DELETE app::t FILTER { id == 1 }");
	insert(&db, &[(5, 2, 3.0, "13:00:00")]);
	assert!(db.await_all_flows(TIMEOUT), "the flows must process the delete");
	await_group(&db, "app::r", 2, &["p50"], vec![vec![bucketed(3.0)]], "the commit after the delete");
	await_group(&db, "app::r", 1, &["p50"], vec![vec![median(&[6.0, 8.0, 9.0])]], "a retraction of an evicted row");
}

#[test]
fn a_late_retraction_older_than_immutable_applies_to_a_rolling_percentile_and_the_min_beside_it() {
	// V31: rows in one rolling slot share a timestamp, so the min never seals and the frame is rebuilt from the
	// rows left.
	let db = setup();
	percentile_view(
		&db,
		"p50: stats::approx_percentile(v, 0.5, 0.01), lo: math::min(v)",
		"p50: Option(float8), lo: Option(float8)",
		"duration: 1h, lateness: 1h, immutable: 5m",
	);
	let columns = ["p50", "lo"];

	insert(&db, &[(1, 1, 1.0, "12:00:00"), (2, 1, 50.0, "12:00:00")]);
	insert(&db, &[(3, 1, 20.0, "12:30:00")]);
	insert(&db, &[(4, 1, 30.0, "12:40:00")]);
	await_group(
		&db,
		"app::r",
		1,
		&columns,
		vec![vec![median(&[1.0, 50.0, 20.0, 30.0]), Value::float8(1.0)]],
		"four rows forty minutes apart",
	);

	db.command("DELETE app::t FILTER { id == 1 }");
	await_group(
		&db,
		"app::r",
		1,
		&columns,
		vec![vec![median(&[50.0, 20.0, 30.0]), Value::float8(20.0)]],
		"a delete forty minutes older than the newest row",
	);

	db.command("UPDATE app::t { v: 2.0 } FILTER { id == 2 }");
	await_group(
		&db,
		"app::r",
		1,
		&columns,
		vec![vec![median(&[2.0, 20.0, 30.0]), Value::float8(2.0)]],
		"an update forty minutes older than the newest row",
	);
}
