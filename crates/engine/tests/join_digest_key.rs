// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, frame::frame::Frame, value_type::ValueType},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t
}

fn digest() -> Digest {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in [1.0, 2.0, 3.0] {
		digest.add_value(&Value::float8(value)).unwrap();
	}
	digest
}

fn with_digest() -> Params {
	let map: HashMap<String, Value> = HashMap::from([("d".to_string(), Value::Digest(Box::new(digest())))]);
	Params::from(map)
}

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().query_as(TestEngine::identity(), rql, with_digest());
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

#[test]
fn inner_join_using_a_digest_key_reports_join_001() {
	// Two digests with equal counts render alike, so matching rows on one would pair unrelated distributions.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: $d } INNER JOIN { FROM test::t | extend { d: $d } } AS s USING (d, s.d)",
	)
	.unwrap_err();

	assert_eq!(err.code, "JOIN_001", "got: {err:?}");
	assert!(err.message.contains("Digest"), "the message must name the digest type, got: {}", err.message);
}

#[test]
fn left_join_using_a_digest_key_reports_join_001() {
	// A left join keeps unmatched rows, so a silently skipped key would still return rows instead of failing.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: $d } LEFT JOIN { FROM test::t | extend { d: $d } } AS s USING (d, s.d)",
	)
	.unwrap_err();

	assert_eq!(err.code, "JOIN_001", "got: {err:?}");
}

#[test]
fn a_digest_key_only_on_the_right_side_reports_join_001() {
	// The build side is keyed before any probe row is read, so it must be checked on its own.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: 1 } INNER JOIN { FROM test::t | extend { d: $d } } AS s USING (d, s.d)",
	)
	.unwrap_err();

	assert_eq!(err.code, "JOIN_001", "got: {err:?}");
}

#[test]
fn a_digest_key_only_on_the_left_side_reports_join_001() {
	// The probe side is keyed per batch, so a digest there must fail even when the build side is scalar.
	let t = engine();

	let err = query(
		&t,
		"FROM test::t | extend { d: $d } INNER JOIN { FROM test::t | extend { d: 1 } } AS s USING (d, s.d)",
	)
	.unwrap_err();

	assert_eq!(err.code, "JOIN_001", "got: {err:?}");
}

#[test]
fn natural_join_sharing_a_digest_column_reports_join_001() {
	// A natural join keys on every shared name, so a shared digest column becomes a key without being named.
	let t = engine();

	let err = query(&t, "FROM test::t | extend { d: $d } NATURAL JOIN { FROM test::t | extend { d: $d } } AS s")
		.unwrap_err();

	assert_eq!(err.code, "JOIN_001", "got: {err:?}");
}

#[test]
fn a_digest_equality_in_a_residual_join_condition_is_an_error_not_a_panic() {
	// Column to column equality is not a hash key, so it runs as a residual whose evaluation error must propagate.
	let t = engine();

	let result =
		query(&t, "FROM test::t | extend { d: $d } INNER JOIN { FROM test::t } AS s USING (a, s.a) AND (d, d)");

	assert!(result.is_err(), "d == d in a residual join condition must fail, got frames: {result:?}");
}

#[test]
fn a_digest_equality_in_a_nested_loop_join_condition_is_an_error_not_a_panic() {
	// An OR sends the whole condition to the nested loop, which evaluates the digest comparison per row pair.
	let t = engine();

	let result = query(
		&t,
		"FROM test::t | extend { d: $d } INNER JOIN { FROM test::t | extend { d: $d } } AS s USING (d, s.d) OR (a, s.a)",
	);

	assert!(result.is_err(), "d == s.d in a nested loop join condition must fail, got frames: {result:?}");
}

#[test]
fn joining_on_a_scalar_key_carries_digest_columns_through() {
	// Only keys are rejected, so a digest riding along on either side must reach the joined rows unchanged.
	let t = engine();

	let frames = query(
		&t,
		"FROM test::t | extend { d: $d } INNER JOIN { FROM test::t | extend { d: $d } } AS s USING (a, s.a)",
	)
	.unwrap();

	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let digests: Vec<Value> = frames[0]
		.columns
		.iter()
		.filter(|c| c.name == "d" || c.name == "s_d")
		.flat_map(|c| (0..c.data.len()).map(|row| c.data.get_value(row)))
		.collect();
	assert_eq!(digests, vec![Value::Digest(Box::new(digest())); 4], "frame:\n{}", frames[0]);
}
