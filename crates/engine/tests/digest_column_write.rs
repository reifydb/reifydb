// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(clippy::result_large_err)]

use std::collections::HashMap;

use reifydb_core::execution::ExecutionResult;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, digest::Digest, frame::frame::Frame, value_type::ValueType},
};

const COLUMN: &str = "Digest(Float8, 0.01)";

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { k: int4, d: digest(float8, 0.01) }");
	t.admin("CREATE TABLE test::o { k: int4, d: Option(digest(float8, 0.01)) }");
	t.admin("CREATE RINGBUFFER test::rb { k: int4, d: digest(float8, 0.01) } WITH { capacity: 10 }");
	t.admin("CREATE SERIES test::s { ts: int8, d: digest(float8, 0.01) } WITH { key: ts }");
	t.admin("CREATE QUEUE test::q { k: int4, d: digest(float8, 0.01) } WITH { fifo: {} }");
	t.admin("CREATE TABLE test::f { k: int4, d: float8 }");
	t.admin("CREATE TABLE test::m { k: int4, d: digest(float8, 0.01), e: digest(float8, 0.05) }");
	t
}

fn digest(inner: ValueType, accuracy: u32, values: &[Value]) -> Value {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	for value in values {
		digest.add_value(value).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn params() -> Params {
	Params::from(HashMap::from([
		("right".to_string(), digest(ValueType::Float8, 10_000, &[Value::float8(1.5), Value::float8(3.0)])),
		("wrong_accuracy".to_string(), digest(ValueType::Float8, 50_000, &[Value::float8(1.5)])),
		("wrong_inner".to_string(), digest(ValueType::Int4, 10_000, &[Value::Int4(7)])),
		("float".to_string(), Value::float8(1.5)),
		("text".to_string(), Value::Utf8("abc".to_string())),
	]))
}

fn run(result: ExecutionResult) -> Result<Vec<Frame>, Diagnostic> {
	match result.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(result.frames),
	}
}

fn command(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Diagnostic> {
	run(t.inner().command_as(TestEngine::identity(), rql, params()))
}

fn wrong_values() -> [(&'static str, &'static str); 4] {
	[
		("$wrong_accuracy", "Digest(Float8, 0.05)"),
		("$wrong_inner", "Digest(Int4, 0.01)"),
		("$float", "Float8"),
		("$text", "Utf8"),
	]
}

fn assert_write_mismatch(err: &Diagnostic, expected: &str, got: &str, fragment: &str, rql: &str) {
	assert_eq!(err.code, "CONSTRAINT_008", "{rql}: {err:?}");
	assert_eq!(err.message, format!("expected {expected}, got {got}"), "{rql}");
	assert_eq!(err.fragment.text(), fragment, "{rql}: the error must point at the written value");
}

fn row(key: &str, key_value: Value, d: Value) -> Params {
	Params::from(HashMap::from([(key.to_string(), key_value), ("d".to_string(), d)]))
}

fn rows(t: &TestEngine, rql: &str) -> usize {
	TestEngine::row_count(&t.query(rql))
}

#[test]
fn inserting_a_mismatched_digest_or_a_non_digest_into_a_table_digest_column_names_both_types() {
	// A cast error blames a cast the user never wrote; the write must say which digest the column wants.
	let t = engine();
	for (value, got) in wrong_values() {
		let rql = format!("INSERT test::t [{{ k: 1, d: {value} }}]");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
	assert_eq!(rows(&t, "FROM test::t"), 0, "a refused write must not leave a row behind");
}

#[test]
fn inserting_a_mismatched_digest_into_an_optional_digest_column_names_the_declared_type() {
	// An Option column must not loosen the digest type: only none or the exact digest may be written.
	let t = engine();
	for (value, got) in wrong_values() {
		let rql = format!("INSERT test::o [{{ k: 1, d: {value} }}]");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, "Option(Digest(Float8, 0.01))", got, value, &rql);
	}
	command(&t, "INSERT test::o [{ k: 1, d: none }, { k: 2, d: $right }]").unwrap();
	assert_eq!(rows(&t, "FROM test::o"), 2);
}

#[test]
fn inserting_a_digest_into_a_non_digest_column_names_both_types() {
	// A digest has no scalar value, so writing one into a float column must fail naming the digest.
	let t = engine();
	let err = command(&t, "INSERT test::f [{ k: 1, d: $right }]").expect_err("digest into float8");
	assert_write_mismatch(&err, "Float8", COLUMN, "$right", "digest into float8");
}

#[test]
fn inserting_a_mismatched_digest_from_a_query_names_both_types() {
	// A digest built by a query reaches the write through a variable, not a literal, and must be checked the same.
	let t = engine();
	t.command("INSERT test::f [{ k: 1, d: 1.5 }, { k: 1, d: 3.0 }]");
	for (source, got, value) in [
		(
			"FROM test::f | aggregate { d: stats::digest(d, 0.05) } by { k }",
			"Digest(Float8, 0.05)",
			"digest(n: 2)",
		),
		(
			"FROM test::f | map { k, x: cast(d, int4) } | aggregate { d: stats::digest(x, 0.01) } by { k }",
			"Digest(Int4, 0.01)",
			"digest(n: 2)",
		),
		("FROM test::f | filter { d < 2.0 }", "Float8", "1.5"),
	] {
		for target in ["test::t", "test::rb", "test::q", "test::o"] {
			let rql = format!("LET $agg = {source}; INSERT {target} $agg");
			let err = command(&t, &rql).expect_err(&rql);
			let expected = if target == "test::o" {
				"Option(Digest(Float8, 0.01))"
			} else {
				COLUMN
			};
			assert_write_mismatch(&err, expected, got, value, &rql);
		}
	}
	assert_eq!(rows(&t, "FROM test::t"), 0);
}

#[test]
fn updating_a_table_digest_column_with_a_mismatched_value_names_both_types() {
	// UPDATE coerces on its own path, so it must refuse the same values INSERT refuses.
	let t = engine();
	command(&t, "INSERT test::t [{ k: 1, d: $right }]").unwrap();
	for (value, got) in wrong_values() {
		let rql = format!("UPDATE test::t {{ d: {value} }} FILTER {{ k == 1 }}");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
	command(&t, "UPDATE test::t { d: $right } FILTER { k == 1 }").unwrap();
}

#[test]
fn inserting_a_mismatched_value_into_a_ringbuffer_digest_column_names_both_types() {
	// A ring buffer builds its rows on its own path, so it must refuse the same values a table refuses.
	let t = engine();
	for (value, got) in wrong_values() {
		let rql = format!("INSERT test::rb [{{ k: 1, d: {value} }}]");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
	assert_eq!(rows(&t, "FROM test::rb"), 0, "a refused write must not leave a row behind");
}

#[test]
fn updating_a_ringbuffer_digest_column_with_a_mismatched_value_names_both_types() {
	// A ring buffer update re-encodes the whole row, so a mismatched digest must stop before the encoder.
	let t = engine();
	command(&t, "INSERT test::rb [{ k: 1, d: $right }]").unwrap();
	for (value, got) in wrong_values() {
		let rql = format!("UPDATE test::rb {{ d: {value} }} FILTER {{ k == 1 }}");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
}

#[test]
fn inserting_a_mismatched_value_into_a_series_digest_column_names_both_types() {
	// A series writes its data columns without the table coercion, so a mismatch must not reach the encoder.
	let t = engine();
	for (value, got) in wrong_values() {
		let rql = format!("INSERT test::s [{{ ts: 1, d: {value} }}]");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
	assert_eq!(rows(&t, "FROM test::s"), 0, "a refused write must not leave a row behind");
}

#[test]
fn updating_a_series_digest_column_with_a_mismatched_value_names_both_types() {
	// A series update re-encodes the row without coercion, so a mismatch must not reach the encoder.
	let t = engine();
	command(&t, "INSERT test::s [{ ts: 1, d: $right }]").unwrap();
	for (value, got) in wrong_values() {
		let rql = format!("UPDATE test::s {{ d: {value} }} FILTER {{ ts == 1 }}");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
}

#[test]
fn inserting_a_mismatched_value_into_a_queue_digest_column_names_both_types() {
	// A queue builds its rows on its own path, so it must refuse the same values a table refuses.
	let t = engine();
	for (value, got) in wrong_values() {
		let rql = format!("INSERT test::q [{{ k: 1, d: {value} }}]");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, value, &rql);
	}
}

#[test]
fn bulk_inserting_a_mismatched_digest_names_both_types() {
	// The bulk path coerces whole columns, so it must refuse the same digest the RQL insert refuses.
	let t = engine();
	let Params::Named(values) = params() else {
		unreachable!("params() builds named parameters")
	};
	for (value, got) in wrong_values() {
		let d = values[value.trim_start_matches('$')].clone();
		for target in ["table", "ringbuffer", "series"] {
			let mut builder = t.bulk_insert(TestEngine::identity());
			match target {
				"table" => builder.table("test::t").row(row("k", Value::Int4(1), d.clone())).done(),
				"ringbuffer" => {
					builder.ringbuffer("test::rb").row(row("k", Value::Int4(1), d.clone())).done()
				}
				_ => builder.series("test::s").row(row("ts", Value::Int8(1), d.clone())).done(),
			};
			let err = builder.execute().expect_err(target).diagnostic();
			assert_write_mismatch(&err, COLUMN, got, "d", &format!("{target} {value}"));
		}
	}
	assert_eq!(rows(&t, "FROM test::t"), 0);
	assert_eq!(rows(&t, "FROM test::rb"), 0);
	assert_eq!(rows(&t, "FROM test::s"), 0);
}

#[test]
fn inserting_a_mismatched_digest_from_a_query_into_a_series_names_both_types() {
	// A series takes a query's rows without coercion, so a mismatch must be refused before the row encoder.
	let t = engine();
	t.command("INSERT test::f [{ k: 1, d: 1.5 }, { k: 2, d: 3.0 }]");
	for (source, got) in [
		("FROM test::f | aggregate { d: stats::digest(d, 0.05) } by { k }", "Digest(Float8, 0.05)"),
		("FROM test::f | aggregate { d: stats::digest(k, 0.01) } by { k }", "Digest(Int4, 0.01)"),
		("FROM test::f", "Float8"),
	] {
		let rql = format!("LET $agg = {source} | map {{ ts: cast(k, int8), d }}; INSERT test::s $agg");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, got, "d", &rql);
	}
	assert_eq!(rows(&t, "FROM test::s"), 0, "a refused write must not leave a row behind");
}

#[test]
fn inserting_a_mismatched_digest_after_a_matching_one_in_one_statement_names_both_types() {
	// Rows of one statement share one column, so a mismatched row must not slip in behind a good one.
	let t = engine();
	for (target, key) in
		[("test::t", "k"), ("test::rb", "k"), ("test::s", "ts"), ("test::q", "k"), ("test::o", "k")]
	{
		for (value, got) in wrong_values() {
			let rql = format!("INSERT {target} [{{ {key}: 1, d: $right }}, {{ {key}: 2, d: {value} }}]");
			let err = command(&t, &rql).expect_err(&rql);
			assert_eq!(err.code, "CONSTRAINT_008", "{rql}: {err:?}");
			assert!(err.message.ends_with(&format!("got {got}")), "{rql}: {}", err.message);
			assert_eq!(
				err.fragment.text(),
				value,
				"{rql}: the error must point at the mismatched row's value"
			);
		}
	}
	assert_eq!(rows(&t, "FROM test::t"), 0, "a refused write must not leave its good row behind");
	assert_eq!(rows(&t, "FROM test::s"), 0, "a refused write must not leave its good row behind");
}

#[test]
fn updating_a_digest_column_from_a_column_of_another_accuracy_names_both_types() {
	// A value read from the row itself reaches the write without a literal, and must be checked the same.
	let t = engine();
	command(&t, "INSERT test::m [{ k: 1, d: $right, e: $wrong_accuracy }]").unwrap();
	let err = command(&t, "UPDATE test::m { d: e } FILTER { k == 1 }").expect_err("d: e");
	assert_write_mismatch(&err, COLUMN, "Digest(Float8, 0.05)", "e", "d: e");
	let err = command(&t, "UPDATE test::m { e: d } FILTER { k == 1 }").expect_err("e: d");
	assert_write_mismatch(&err, "Digest(Float8, 0.05)", COLUMN, "d", "e: d");
}

#[test]
fn inserting_from_a_table_of_another_digest_names_both_types() {
	// A FROM source feeds stored rows to the insert, so no literal is cast on the way in.
	let t = engine();
	t.admin("CREATE TABLE test::wk { k: int4, d: digest(float8, 0.05) }");
	t.admin("CREATE TABLE test::wts { ts: int8, d: digest(float8, 0.05) }");
	command(&t, "INSERT test::wk [{ k: 1, d: $wrong_accuracy }]").unwrap();
	command(&t, "INSERT test::wts [{ ts: 1, d: $wrong_accuracy }]").unwrap();
	for (target, source, fragment) in [
		("test::t", "test::wk", "digest(n: 1)"),
		("test::rb", "test::wk", "digest(n: 1)"),
		("test::q", "test::wk", "digest(n: 1)"),
		("test::s", "test::wts", "d"),
	] {
		let rql = format!("INSERT {target} FROM {source}");
		let err = command(&t, &rql).expect_err(&rql);
		assert_write_mismatch(&err, COLUMN, "Digest(Float8, 0.05)", fragment, &rql);
	}
	assert_eq!(rows(&t, "FROM test::t"), 0, "a refused write must not leave a row behind");
	assert_eq!(rows(&t, "FROM test::rb"), 0, "a refused write must not leave a row behind");
	assert_eq!(rows(&t, "FROM test::s"), 0, "a refused write must not leave a row behind");
}

#[test]
fn a_write_with_returning_refuses_a_mismatched_digest_before_returning_a_row() {
	// RETURNING must never echo a row whose digest the column could not hold.
	let t = engine();
	command(&t, "INSERT test::t [{ k: 1, d: $right }]").unwrap();
	command(&t, "INSERT test::s [{ ts: 1, d: $right }]").unwrap();
	for rql in [
		"INSERT test::t [{ k: 2, d: $wrong_accuracy }] RETURNING { k, d }",
		"INSERT test::rb [{ k: 2, d: $wrong_accuracy }] RETURNING { k, d }",
		"INSERT test::s [{ ts: 2, d: $wrong_accuracy }] RETURNING { ts, d }",
		"UPDATE test::t { d: $wrong_accuracy } FILTER { k == 1 } RETURNING { k, d }",
		"UPDATE test::s { d: $wrong_accuracy } FILTER { ts == 1 } RETURNING { ts, d }",
	] {
		let err = command(&t, rql).expect_err(rql);
		assert_write_mismatch(&err, COLUMN, "Digest(Float8, 0.05)", "$wrong_accuracy", rql);
	}
}

#[test]
fn bulk_inserting_a_mismatched_digest_after_a_matching_one_names_both_types() {
	// Bulk rows are gathered into one column first, so a mismatch in a later row must be an error, not a panic.
	let t = engine();
	let Params::Named(values) = params() else {
		unreachable!("params() builds named parameters")
	};
	let right = values["right"].clone();
	for (value, got) in wrong_values() {
		let d = values[value.trim_start_matches('$')].clone();
		for target in ["table", "ringbuffer", "series", "positional table"] {
			let mut builder = t.bulk_insert(TestEngine::identity());
			match target {
				"table" => builder
					.table("test::t")
					.row(row("k", Value::Int4(1), right.clone()))
					.row(row("k", Value::Int4(2), d.clone()))
					.done(),
				"ringbuffer" => builder
					.ringbuffer("test::rb")
					.row(row("k", Value::Int4(1), right.clone()))
					.row(row("k", Value::Int4(2), d.clone()))
					.done(),
				"series" => builder
					.series("test::s")
					.row(row("ts", Value::Int8(1), right.clone()))
					.row(row("ts", Value::Int8(2), d.clone()))
					.done(),
				_ => builder
					.table("test::t")
					.row(Params::from(vec![Value::Int4(1), right.clone()]))
					.row(Params::from(vec![Value::Int4(2), d.clone()]))
					.done(),
			};
			let err = builder.execute().expect_err(target).diagnostic();
			assert_write_mismatch(&err, COLUMN, got, "d", &format!("{target} {value}"));
		}
	}
	assert_eq!(rows(&t, "FROM test::t"), 0);
}

#[test]
fn a_table_keyed_by_a_digest_or_optional_digest_column_refuses_rows_naming_the_column() {
	// A digest has no key encoding; an optional digest key would give every row the same none key.
	let t = engine();
	t.admin("CREATE TABLE test::pk { k: int4, d: digest(float8, 0.01) }");
	t.admin("CREATE PRIMARY KEY ON test::pk { d }");
	t.admin("CREATE TABLE test::opk { k: int4, d: Option(digest(float8, 0.01)) }");
	t.admin("CREATE PRIMARY KEY ON test::opk { k, d }");
	for (rql, ty) in [
		("INSERT test::pk [{ k: 1, d: $right }]", COLUMN),
		("INSERT test::opk [{ k: 1, d: $right }]", "Option(Digest(Float8, 0.01))"),
		("INSERT test::opk [{ k: 1, d: none }]", "Option(Digest(Float8, 0.01))"),
	] {
		let err = command(&t, rql).expect_err(rql);
		assert_eq!(err.code, "INDEX_003", "{rql}: {err:?}");
		assert_eq!(
			err.message,
			format!("cannot use column `d` in a primary key: a {ty} value cannot be a key"),
			"{rql}"
		);
		assert_eq!(err.fragment.text(), "d", "{rql}: the error must name the digest key column");
	}
	let Params::Named(values) = params() else {
		unreachable!("params() builds named parameters")
	};
	let mut builder = t.bulk_insert(TestEngine::identity());
	builder.table("test::pk").row(row("k", Value::Int4(1), values["right"].clone())).done();
	let err = builder.execute().expect_err("bulk insert into a digest keyed table").diagnostic();
	assert_eq!(err.code, "INDEX_003", "{err:?}");
	assert_eq!(err.fragment.text(), "d", "the error must name the digest key column");
	assert_eq!(rows(&t, "FROM test::pk"), 0);
	assert_eq!(rows(&t, "FROM test::opk"), 0);
}

#[test]
fn inserting_text_from_a_query_into_a_series_float_column_fails_as_it_does_for_a_table() {
	// A series takes a query's rows without coercion, so any type mismatch reaches the row encoder and panics.
	let t = engine();
	t.admin("CREATE TABLE test::tx { ts: int8, v: utf8 }");
	t.admin("CREATE TABLE test::tf { ts: int8, v: float8 }");
	t.admin("CREATE SERIES test::sf { ts: int8, v: float8 } WITH { key: ts }");
	t.command("INSERT test::tx [{ ts: 1, v: 'abc' }]");
	let table =
		command(&t, "LET $rows = FROM test::tx; INSERT test::tf $rows").expect_err("text into a table float");
	let series =
		command(&t, "LET $rows = FROM test::tx; INSERT test::sf $rows").expect_err("text into a series float");
	assert_eq!(series.code, table.code, "{series:?}");
	assert_eq!(rows(&t, "FROM test::sf"), 0);
}

#[test]
fn bulk_inserting_text_after_a_float_into_a_float_column_is_an_error_not_a_panic() {
	// Bulk rows are gathered into one column first, so a later row of another type must not panic the push.
	let t = engine();
	let mut alone = t.bulk_insert(TestEngine::identity());
	alone.table("test::f").row(row("k", Value::Int4(2), Value::Utf8("abc".to_string()))).done();
	let alone = alone.execute().expect_err("text into float8").diagnostic();
	let mut builder = t.bulk_insert(TestEngine::identity());
	builder.table("test::f")
		.row(row("k", Value::Int4(1), Value::float8(1.5)))
		.row(row("k", Value::Int4(2), Value::Utf8("abc".to_string())))
		.done();
	let err = builder.execute().expect_err("text after a float into float8").diagnostic();
	assert_eq!(err.code, alone.code, "{err:?}");
	assert_eq!(rows(&t, "FROM test::f"), 0);
}
