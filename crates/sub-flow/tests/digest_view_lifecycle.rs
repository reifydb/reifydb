// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration as StdDuration;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb::{
	Frame, SqliteConfig, Value, WithSubsystem, embedded,
	testing::db::{TempDbPath, TestDb, await_value},
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_test_harness::assert::{column_values, rows};
use reifydb_value::value::value_type::ValueType;

const TIMEOUT: StdDuration = StdDuration::from_secs(60);
const ROWS: usize = 3000;
const CHUNK: usize = 500;
const SEED: u64 = 0xD16E_57F1_0A7E_0042;
const DIGEST: &str = "stats::digest(latency, 0.01)";
const PERCENTILES: [(&str, &str); 4] = [("p0", "0"), ("p50", "0.5"), ("p99", "0.99"), ("p100", "1")];

type Canonical = (ValueType, u32, u64, Vec<u8>);

#[derive(Clone, Copy)]
enum Latency {
	Float8,
	Int4,
	Duration,
}

impl Latency {
	fn ddl(self) -> &'static str {
		match self {
			Latency::Float8 => "float8",
			Latency::Int4 => "int4",
			Latency::Duration => "duration",
		}
	}

	fn random(self, rng: &mut StdRng) -> String {
		if rng.random_range(0..10) == 0 {
			return "none".to_string();
		}
		match self {
			Latency::Float8 => {
				let magnitude =
					rng.random_range(1..1_000_000i64) as f64 / 10f64.powi(rng.random_range(0..5));
				let sign = if rng.random_range(0..4) == 0 {
					-1.0
				} else {
					1.0
				};
				format!("{:?}", sign * magnitude)
			}
			Latency::Int4 => rng.random_range(-50_000..1_000_000i32).to_string(),
			Latency::Duration => format!("{}ms", rng.random_range(0..10_000_000i64)),
		}
	}

	fn edges(self) -> [&'static str; 4] {
		match self {
			Latency::Float8 => ["-2.5", "0.0", "none", "7.25"],
			Latency::Int4 => ["-3", "0", "none", "12"],
			Latency::Duration => ["0ms", "0ms", "none", "90000000ms"],
		}
	}

	fn moved(self) -> &'static str {
		match self {
			Latency::Float8 => "123456.5",
			Latency::Int4 => "-777",
			Latency::Duration => "42ms",
		}
	}
}

fn runtime() -> RuntimeConfig {
	RuntimeConfig::default().fatal(FatalConfig::disarmed())
}

fn memory() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(runtime())
			.with_flow(|f| f)
			.build()
			.expect("build memory db with flow"),
	)
}

fn sqlite(path: &TempDbPath) -> TestDb {
	TestDb::from(
		embedded::sqlite(SqliteConfig::new(path))
			.with_runtime_config(runtime())
			.with_flow(|f| f)
			.build()
			.expect("build sqlite db with flow"),
	)
}

fn declare(db: &TestDb, latency: Latency) {
	let ty = latency.ddl();
	db.admin("CREATE NAMESPACE app");
	db.admin(&format!(
		"CREATE TABLE app::t {{ id: int4, g: int4, ws: datetime, ts: datetime, weight: float8, latency: Option({ty}) }} with {{ time: event(ts) }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::windowed {{ g: int4, s: datetime, d: Option(digest({ty}, 0.01)) }} AS {{ FROM app::t | window tumbling {{ s: window::start(), d: {DIGEST} }} by {{ g }} with {{ duration: 60s, lateness: 3600s }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::grouped {{ g: int4, d: Option(digest({ty}, 0.01)) }} AS {{ FROM app::t | aggregate {{ d: {DIGEST} }} by {{ g }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::whole {{ d: Option(digest({ty}, 0.01)) }} AS {{ FROM app::t | aggregate {{ d: {DIGEST} }} }}"
	));
	db.admin(&format!(
		"CREATE DEFERRED VIEW app::merged {{ d: Option(digest({ty}, 0.01)) }} AS {{ FROM app::grouped | aggregate {{ d: stats::digest(d) }} }}"
	));
	db.admin(
		"CREATE DEFERRED VIEW app::sums { g: int4, s: Option(float8) } AS { FROM app::t | aggregate { s: math::sum(weight) } by { g } }",
	);
}

fn corpus(latency: Latency) -> Vec<String> {
	let mut rng = StdRng::seed_from_u64(SEED);
	(0..ROWS)
		.map(|id| {
			let (g, w, value) = match latency.edges().get(id) {
				Some(edge) => (5, 0, edge.to_string()),
				None => (rng.random_range(1..5), rng.random_range(0..3), latency.random(&mut rng)),
			};
			let second = rng.random_range(0..60);
			let milli = rng.random_range(0..1000);
			format!(
				r#"{{ id: {id}, g: {g}, ws: "2026-01-01T00:{w:02}:00Z", ts: "2026-01-01T00:{w:02}:{second:02}.{milli:03}Z", weight: 1.0, latency: {value} }}"#
			)
		})
		.collect()
}

fn insert(db: &TestDb, rows: &[String]) {
	for chunk in rows.chunks(CHUNK) {
		db.command(&format!("INSERT app::t [{}]", chunk.join(", ")));
	}
}

fn keyed(frames: &[Frame], keys: &[&str], values: &[&str]) -> Vec<(Vec<String>, Vec<Value>)> {
	let mut out: Vec<(Vec<String>, Vec<Value>)> = rows(frames)
		.into_iter()
		.map(|row| {
			let cell = |name: &str| {
				row.iter()
					.find(|(column, _)| column == name)
					.map(|(_, value)| value.clone())
					.unwrap_or_else(|| panic!("row has no column {name}: {row:?}"))
			};
			(
				keys.iter().map(|key| cell(key).to_string()).collect(),
				values.iter().map(|v| cell(v)).collect(),
			)
		})
		.collect();
	out.sort_by(|a, b| a.0.cmp(&b.0));
	out
}

fn canonical(value: &Value) -> Option<Canonical> {
	match value {
		Value::Digest(digest) => {
			Some((digest.inner().clone(), digest.accuracy(), digest.count(), digest.encode()))
		}
		Value::None {
			..
		} => None,
		other => panic!("expected a digest or none, got {other:?}"),
	}
}

fn digests(db: &TestDb, rql: &str, keys: &[&str]) -> Vec<(Vec<String>, Option<Canonical>)> {
	keyed(&db.query(rql), keys, &["d"]).into_iter().map(|(key, values)| (key, canonical(&values[0]))).collect()
}

fn summary(rows: &[(Vec<String>, Option<Canonical>)]) -> Vec<(&[String], Option<(&ValueType, u32, u64, usize)>)> {
	rows.iter()
		.map(|(key, digest)| {
			(
				key.as_slice(),
				digest.as_ref().map(|(inner, ppm, count, bytes)| (inner, *ppm, *count, bytes.len())),
			)
		})
		.collect()
}

fn assert_views_match_batch(db: &TestDb, step: &str) {
	assert!(db.await_all_flows(TIMEOUT), "{step}: the flows must process every change");
	assert_stored_views_match_batch(db, step);
}

fn assert_stored_views_match_batch(db: &TestDb, step: &str) {
	let grouped_batch = format!("FROM app::t | aggregate {{ d: {DIGEST} }} by {{ g }}");
	let digest_pairs: [(&str, &[&str], String, &[&str]); 5] = [
		(
			"FROM app::windowed",
			&["s", "g"],
			format!("FROM app::t | aggregate {{ d: {DIGEST} }} by {{ ws, g }}"),
			&["ws", "g"],
		),
		("FROM app::grouped", &["g"], grouped_batch.clone(), &["g"]),
		("FROM app::whole", &[], format!("FROM app::t | aggregate {{ d: {DIGEST} }}"), &[]),
		("FROM app::merged", &[], format!("{grouped_batch} | aggregate {{ d: stats::digest(d) }}"), &[]),
		(
			"FROM app::grouped | aggregate { d: stats::digest(d) }",
			&[],
			format!("FROM app::t | aggregate {{ d: {DIGEST} }}"),
			&[],
		),
	];
	for (view, view_keys, batch, batch_keys) in &digest_pairs {
		let expected = digests(db, batch, batch_keys);
		assert!(!expected.is_empty(), "{step}: the batch rebuild {batch} must not be empty");
		let got = await_value(expected.clone(), TIMEOUT, || digests(db, view, view_keys));
		assert!(
			got == expected,
			"{step}: {view} must equal the batch rebuild {batch}\nview:  {:?}\nbatch: {:?}",
			summary(&got),
			summary(&expected)
		);
	}

	let names: Vec<&str> = PERCENTILES.iter().map(|(name, _)| *name).collect();
	let reads = PERCENTILES.map(|(name, p)| format!("{name}: stats::approx_percentile(d, {p})")).join(", ");
	let source_reads =
		PERCENTILES.map(|(name, p)| format!("{name}: stats::approx_percentile(latency, {p}, 0.01)")).join(", ");
	let percentile_pairs: [(String, &[&str], String, &[&str]); 4] = [
		(
			format!("FROM app::windowed | map {{ s, g, {reads} }}"),
			&["s", "g"],
			format!("FROM app::t | aggregate {{ {source_reads} }} by {{ ws, g }}"),
			&["ws", "g"],
		),
		(
			format!("FROM app::grouped | map {{ g, {reads} }}"),
			&["g"],
			format!("FROM app::t | aggregate {{ {source_reads} }} by {{ g }}"),
			&["g"],
		),
		(
			format!("FROM app::whole | map {{ {reads} }}"),
			&[],
			format!("FROM app::t | aggregate {{ {source_reads} }}"),
			&[],
		),
		(
			format!("FROM app::merged | map {{ {reads} }}"),
			&[],
			format!("FROM app::t | aggregate {{ {source_reads} }}"),
			&[],
		),
	];
	for (view, view_keys, batch, batch_keys) in &percentile_pairs {
		let expected = keyed(&db.query(batch), batch_keys, &names);
		let got = keyed(&db.query(view), view_keys, &names);
		assert_eq!(got, expected, "{step}: {view} must read the same percentiles as {batch}");
	}
}

fn assert_digests_are_not_trivial(db: &TestDb, rows: &[String]) {
	let present = rows.iter().filter(|row| !row.contains("latency: none")).count() as u64;
	let whole = column_values(&db.query("FROM app::whole")[0], "d");
	let Value::Digest(digest) = &whole[0] else {
		panic!("the whole view must hold a digest, got {whole:?}");
	};
	assert_eq!(digest.count(), present, "the whole digest must count every present source value");

	let grouped = keyed(&db.query("FROM app::grouped"), &["g"], &["d"]);
	assert_eq!(grouped.len(), 5, "the corpus must fill five groups, got {:?}", summary_keys(&grouped));
	for (key, values) in grouped.iter().filter(|(key, _)| key[0] != "5") {
		let Value::Digest(digest) = &values[0] else {
			panic!("group {key:?} must hold a digest, got {values:?}");
		};
		assert!(
			digest.bucket_count() > 100,
			"group {key:?} must spread over many buckets, got {}",
			digest.bucket_count()
		);
	}
}

fn summary_keys(rows: &[(Vec<String>, Vec<Value>)]) -> Vec<&[String]> {
	rows.iter().map(|(key, _)| key.as_slice()).collect()
}

fn groups(db: &TestDb, view: &str) -> Vec<String> {
	keyed(&db.query(view), &["g"], &[]).into_iter().map(|(key, _)| key[0].clone()).collect()
}

fn lifecycle(latency: Latency) {
	let db = memory();
	declare(&db, latency);
	let rows = corpus(latency);

	insert(&db, &rows);
	assert_views_match_batch(&db, "insert");
	assert_digests_are_not_trivial(&db, &rows);

	db.command("DELETE app::t FILTER { id >= 200 and id < 900 }");
	assert_views_match_batch(&db, "delete a range");

	db.command(&format!("UPDATE app::t {{ latency: {} }} FILTER {{ id >= 1000 and id < 1100 }}", latency.moved()));
	assert_views_match_batch(&db, "update values within their groups");

	db.command("UPDATE app::t { latency: none } FILTER { id >= 1100 and id < 1150 }");
	assert_views_match_batch(&db, "update values to none");

	db.command("UPDATE app::t { g: 2 } FILTER { g == 1 and id >= 1500 and id < 2200 }");
	assert_views_match_batch(&db, "update rows into another group");

	assert!(groups(&db, "FROM app::sums").contains(&"5".to_string()), "group 5 must exist in the sum view first");
	assert!(
		groups(&db, "FROM app::grouped").contains(&"5".to_string()),
		"group 5 must exist in the digest view first"
	);
	db.command("DELETE app::t FILTER { g == 5 }");
	assert_views_match_batch(&db, "delete every row of a group");
	assert_eq!(groups(&db, "FROM app::sums"), vec!["1", "2", "3", "4"], "the sum view must drop the emptied group");
	assert_eq!(
		groups(&db, "FROM app::grouped"),
		groups(&db, "FROM app::sums"),
		"the digest view must drop an emptied group exactly as the sum view does"
	);
}

fn restart(latency: Latency) {
	let path = TempDbPath::new("digest_view_restart");
	let rows = corpus(latency);
	let (before, after) = rows.split_at(ROWS / 2);
	{
		let mut db = sqlite(&path);
		declare(&db, latency);
		insert(&db, before);
		assert_views_match_batch(&db, "before the restart");
		db.stop();
	}

	let mut db = sqlite(&path);
	assert_stored_views_match_batch(&db, "right after the restart");
	insert(&db, after);
	db.command("DELETE app::t FILTER { id >= 200 and id < 900 }");
	db.command("UPDATE app::t { g: 2 } FILTER { g == 1 and id >= 1000 and id < 2200 }");
	assert_views_match_batch(&db, "changes after the restart");
	db.stop();
}

#[test]
fn a_float8_digest_view_equals_a_batch_rebuild_through_inserts_deletes_and_updates() {
	// Every retraction must leave the stored digest equal to a rebuild, otherwise percentiles drift.
	lifecycle(Latency::Float8);
}

#[test]
fn an_int4_digest_view_equals_a_batch_rebuild_through_inserts_deletes_and_updates() {
	// An int4 digest must keep its inner type through retractions, otherwise a batch merge with it fails.
	lifecycle(Latency::Int4);
}

#[test]
fn a_duration_digest_view_equals_a_batch_rebuild_through_inserts_deletes_and_updates() {
	// A duration digest must keep zero-length values and its inner type, otherwise reads lose their unit.
	lifecycle(Latency::Duration);
}

#[test]
fn a_float8_digest_view_keeps_its_slot_state_across_a_restart() {
	// A retraction of a row stored before the restart must find it in the reloaded slot, otherwise it panics.
	restart(Latency::Float8);
}

#[test]
fn an_int4_digest_view_keeps_its_slot_state_across_a_restart() {
	// A reloaded int4 slot must keep its inner type, otherwise the first merge after the restart fails.
	restart(Latency::Int4);
}

#[test]
fn a_duration_digest_view_keeps_its_slot_state_across_a_restart() {
	// A reloaded duration slot must keep its inner type, otherwise the first merge after the restart fails.
	restart(Latency::Duration);
}
