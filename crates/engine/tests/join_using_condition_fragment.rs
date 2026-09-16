// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, value_type::ValueType},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 1, b: 10 }, { a: 2, b: 20 }]");
	t
}

fn with_digest() -> Params {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	digest.add_value(&Value::float8(1.0)).unwrap();
	Params::from(HashMap::from([("d".to_string(), Value::Digest(Box::new(digest)))]))
}

fn query_err(t: &TestEngine, rql: &str) -> Diagnostic {
	let r = t.inner().query_as(TestEngine::identity(), rql, with_digest());
	match r.error {
		Some(e) => e.diagnostic(),
		None => panic!("expected an error, got frames {:?}\nrql: {rql}", r.frames),
	}
}

fn assert_fragment_is_the_source_text(err: &Diagnostic, rql: &str, pair: &str) {
	let text = err.fragment.text();
	let offset = (*err.fragment.column() as usize)
		.checked_sub(1)
		.unwrap_or_else(|| panic!("the fragment must carry its position in the query, got {:?}", err.fragment));
	assert!(
		rql.get(offset..).is_some_and(|rest| rest.starts_with(text)),
		"the fragment must be the query text at its own position, got {text:?} at column {}\nrql: {rql}",
		offset + 1
	);
	assert!(text.contains(pair), "the fragment must cover the pair `{pair}` as written, got {text:?}\nrql: {rql}");
}

#[test]
fn a_residual_join_condition_error_quotes_the_condition_as_written() {
	// The fragment must be a slice of the query with its spaces, never the token texts glued together.
	let t = engine();
	let rql = "FROM test::t | extend { d: $d } INNER JOIN { FROM test::t } AS s USING (a, s.a) AND (d, d)";

	let err = query_err(&t, rql);

	assert_eq!(err.code, "OPERATOR_022", "got: {err:?}");
	assert_fragment_is_the_source_text(&err, rql, "d, d");
}

#[test]
fn a_nested_loop_join_condition_error_quotes_the_condition_as_written() {
	// A qualified column must be quoted where it is written, never glued to the alias declared before USING.
	let t = engine();
	let rql = "FROM test::t | extend { d: $d } INNER JOIN { FROM test::t | extend { d: $d } } AS s USING (d, s.d) OR (a, s.a)";

	let err = query_err(&t, rql);

	assert_eq!(err.code, "OPERATOR_022", "got: {err:?}");
	assert_fragment_is_the_source_text(&err, rql, "d, s.d");
}

#[test]
fn a_join_condition_type_mismatch_quotes_the_condition_as_written() {
	// A literal operand must keep its quotes and spacing in the fragment, never read as USINGbx.
	let t = engine();
	let rql = "FROM test::t INNER JOIN { FROM test::t } AS s USING (a, s.a) AND (b, 'x')";

	let err = query_err(&t, rql);

	assert_eq!(err.code, "OPERATOR_022", "got: {err:?}");
	assert_fragment_is_the_source_text(&err, rql, "b, 'x'");
}
