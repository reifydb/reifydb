// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, duration::Duration, frame::frame::Frame, value_type::ValueType},
};

const ACCURACY: u32 = 10_000;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 2, b: 1 }, { a: 1, b: 2 }]");
	t
}

fn float_digest() -> Digest {
	let mut digest = Digest::new(ValueType::Float8, ACCURACY).unwrap();
	for value in [10.0, 20.0, 30.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	digest
}

fn duration_digest() -> Digest {
	let mut digest = Digest::new(ValueType::Duration, ACCURACY).unwrap();
	for ms in [5, 50, 500, 5000] {
		digest.add_value(&Value::Duration(Duration::from_milliseconds(ms).unwrap())).unwrap();
	}
	digest
}

fn with_digest(digest: &Digest) -> Params {
	let map: HashMap<String, Value> = HashMap::from([("d".to_string(), Value::Digest(Box::new(digest.clone())))]);
	Params::from(map)
}

fn query(t: &TestEngine, rql: &str, params: Params) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no column {name}"));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

#[test]
fn sorting_by_a_digest_column_reports_sort_002_instead_of_panicking() {
	// A digest has no order, so sort must reject the column before any compare runs.
	let t = engine();

	let err = query(&t, "FROM test::t | extend { d: $d } | sort { d }", with_digest(&float_digest())).unwrap_err();

	assert_eq!(err.code, "SORT_002", "got: {err:?}");
}

#[test]
fn top_k_by_a_digest_column_reports_sort_002_instead_of_panicking() {
	// Sort followed by take runs the top-k node, which compares values on its own path.
	let t = engine();

	let err = query(&t, "FROM test::t | extend { d: $d } | sort { d } | take 1", with_digest(&float_digest()))
		.unwrap_err();

	assert_eq!(err.code, "SORT_002", "got: {err:?}");
}

#[test]
fn grouping_by_a_digest_column_reports_aggregate_008_instead_of_panicking() {
	// A digest has no key encoding, so aggregate must reject it as a group key before hashing rows.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: $d } | aggregate { n: math::count(a) } by { d }",
		with_digest(&float_digest()),
	)
	.unwrap_err();

	assert_eq!(err.code, "AGGREGATE_008", "got: {err:?}");
}

#[test]
fn distinct_on_a_digest_column_is_an_error_not_a_panic() {
	// Distinct keys rows like a group, so a digest must fail there with an error too.
	let t = engine();

	let err = query(&t, "FROM test::t | extend { d: $d } | distinct { d }", with_digest(&float_digest()))
		.unwrap_err();

	assert_eq!(err.code, "DISTINCT_001", "got: {err:?}");
}

#[test]
fn distinct_over_all_columns_including_a_digest_is_an_error() {
	// Distinct compares rendered values, and two different digests with the same count render alike.
	let t = engine();

	let err = query(&t, "FROM test::t | extend { d: $d } | distinct {}", with_digest(&float_digest())).unwrap_err();

	assert_eq!(err.code, "DISTINCT_001", "got: {err:?}");
}

#[test]
fn a_digest_parameter_reaches_the_result_and_renders_as_its_count() {
	let t = engine();

	let frames = query(&t, "FROM test::t | extend { d: $d } | map { a, d }", with_digest(&float_digest())).unwrap();

	assert_eq!(column_values(&frames, "d"), vec![Value::Digest(Box::new(float_digest())); 2]);
	assert!(frames[0].to_string().contains("digest(n: 3)"), "rendered:\n{}", frames[0]);
}

#[test]
fn approx_percentile_in_map_reads_p_from_each_row_of_a_float8_digest() {
	// p comes from a column here, so each row must use its own p against the same digest.
	let t = engine();
	let digest = float_digest();

	let frames = query(
		&t,
		"FROM test::t | extend { d: $d } | map { a, p: stats::approx_percentile(d, if a == 1 { 0.0 } else { 1.0 }) }",
		with_digest(&digest),
	)
	.unwrap();

	let a = column_values(&frames, "a");
	let p = column_values(&frames, "p");
	for (row, a) in a.iter().enumerate() {
		let expected_p = if *a == Value::Int4(1) {
			0.0
		} else {
			1.0
		};
		assert_eq!(p[row], digest.percentile_value(expected_p).unwrap(), "row {row} with a {a}");
	}
	assert_ne!(p[0], p[1], "p 0 and p 1 over three distinct values must differ");
}

#[test]
fn approx_percentile_in_map_returns_duration_for_a_duration_digest() {
	let t = engine();
	let digest = duration_digest();

	let frames = query(
		&t,
		"FROM test::t | extend { d: $d } | map { p: stats::approx_percentile(d, 0.75) }",
		with_digest(&digest),
	)
	.unwrap();

	let expected = digest.percentile_value(0.75).unwrap();
	assert!(matches!(expected, Value::Duration(_)));
	assert_eq!(column_values(&frames, "p"), vec![expected; 2]);
}

#[test]
fn approx_percentile_in_map_with_a_literal_p_out_of_range_is_an_error() {
	// Clamping 1.5 would answer a different question than the one asked.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: $d } | map { p: stats::approx_percentile(d, 1.5) }",
		with_digest(&float_digest()),
	)
	.unwrap_err();

	assert!(format!("{err:?}").contains("p must be between 0 and 1"), "got: {err:?}");
}

#[test]
fn approx_percentile_in_map_on_a_raw_column_is_an_error() {
	// No digest is built outside window and aggregate, so a raw column must not be accepted.
	let t = engine();

	let err = query(&t, "FROM test::t | map { p: stats::approx_percentile(a, 0.5, 0.01) }", Params::None)
		.unwrap_err();

	assert!(format!("{err:?}").contains("only supported inside window or aggregate"), "got: {err:?}");
}

#[test]
fn comparing_two_digests_with_equals_is_an_error_not_a_panic() {
	// Two digests with equal counts render alike, so equality must not be answered from a rendering.
	let t = engine();

	let result = query(&t, "FROM test::t | extend { d: $d } | map { same: d == d }", with_digest(&float_digest()));

	assert!(result.is_err(), "d == d on a digest must fail, got frames: {result:?}");
}
