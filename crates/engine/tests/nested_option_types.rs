// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t
}

fn nested(depth: usize) -> String {
	(0..depth).fold("int4".to_string(), |inner, _| format!("Option({inner})"))
}

fn admin(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn query(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().query_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn assert_nested_option_refused(result: Result<Vec<Frame>, Box<Diagnostic>>, rql: &str) {
	let err = match result {
		Err(err) => err,
		Ok(frames) => panic!("expected the nested Option to be refused, got {frames:?}\nrql: {rql}"),
	};
	assert_eq!(err.code, "AST_020", "{rql}: {err:?}");
	assert_eq!(err.fragment.text(), "int4", "{rql}: the error must point into the nested type: {err:?}");
}

fn column_values(frames: &[Frame], name: &str) -> Vec<Value> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0]
		.columns
		.iter()
		.find(|c| c.name == name)
		.unwrap_or_else(|| panic!("column {name} missing from {:?}", frames[0].columns));
	(0..column.data.len()).map(|row| column.data.get_value(row)).collect()
}

#[test]
fn a_column_nested_past_the_type_tag_is_refused_at_create() {
	// The type tag holds three Option layers, so a fourth panicked the catalog while storing the column.
	let t = engine();
	t.admin("CREATE TABLE test::existing { a: int4 }");
	let d = nested(4);
	for rql in [
		format!("CREATE TABLE test::t {{ v: {d} }}"),
		format!("CREATE RINGBUFFER test::rb {{ v: {d} }} WITH {{ capacity: 4 }}"),
		format!("CREATE SERIES test::s {{ ts: int8, v: {d} }} WITH {{ key: ts }}"),
		format!("CREATE QUEUE test::q {{ v: {d} }} WITH {{ fifo: {{}} }}"),
		format!("CREATE DEFERRED VIEW test::v {{ v: {d} }} AS {{ MAP {{ v: 1 }} }}"),
		format!("CREATE DICTIONARY test::d FOR {d} AS uint4"),
		format!("ALTER TABLE test::existing ADD COLUMN b: {d}"),
	] {
		assert_nested_option_refused(admin(&t, &rql), &rql);
	}
}

#[test]
fn an_option_of_option_column_is_refused_before_a_row_can_reach_it() {
	// A row stores one none bit per field, so a second layer panicked the first insert.
	let t = engine();
	let rql = format!("CREATE TABLE test::t {{ v: {} }}", nested(2));

	assert_nested_option_refused(admin(&t, &rql), &rql);
}

#[test]
fn a_variant_field_nested_past_the_type_tag_is_refused_at_create() {
	// A variant listed with a field the type tag cannot hold panicked every read of the variants table.
	let t = engine();
	let d = nested(4);
	for rql in [
		format!("CREATE ENUM test::e {{ A {{ x: {d} }} }}"),
		format!("CREATE EVENT test::ev {{ A {{ x: {d} }} }}"),
		format!("CREATE TAG test::tg {{ A {{ x: {d} }} }}"),
	] {
		assert_nested_option_refused(admin(&t, &rql), &rql);
	}
}

#[test]
fn a_procedure_parameter_nesting_option_in_option_is_refused() {
	// Every type annotation shares one limit, so a parameter must not accept what a column refuses.
	let t = engine();
	let rql = format!("CREATE PROCEDURE test::p {{ n: {} }} AS {{ MAP {{ out: $n }} }}", nested(2));

	assert_nested_option_refused(admin(&t, &rql), &rql);
}

#[test]
fn a_udf_typed_option_of_option_is_refused() {
	// Returning none into a second Option layer hit an unreachable while building the result column.
	let t = engine();
	let d = nested(2);
	for rql in [
		format!("UDF f ($x: int4): {d} {{ RETURN none }}; MAP {{ v: f(5) }}"),
		format!("UDF f ($x: {d}) {{ RETURN $x }}; MAP {{ v: f(none) }}"),
	] {
		assert_nested_option_refused(query(&t, &rql), &rql);
	}
}

#[test]
fn an_optional_enum_field_stores_a_value_and_a_none() {
	// Wrapping an already optional field in a second Option panicked the first insert of the variant.
	let t = engine();
	t.admin("CREATE ENUM test::e { A { x: Option(int4) }, B }");
	t.admin("CREATE TABLE test::t { id: int4, v: test::e }");
	t.command("INSERT test::t [{ id: 1, v: test::e::A { x: 5 } }]");
	t.command("INSERT test::t [{ id: 2, v: test::e::A { x: none } }]");
	t.command("INSERT test::t [{ id: 3, v: test::e::B }]");

	let frames = t.query("FROM test::t SORT { id: ASC }");

	assert_eq!(column_values(&frames, "v_tag"), vec![Value::Uint1(0), Value::Uint1(0), Value::Uint1(1)]);
	let x = column_values(&frames, "v_a_x");
	assert_eq!(x[0], Value::Int4(5), "the stored field value must read back: {x:?}");
	assert!(matches!(x[1], Value::None { .. }), "an A with no x must read back as none: {x:?}");
	assert!(matches!(x[2], Value::None { .. }), "a B has no x: {x:?}");
}
