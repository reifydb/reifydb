// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{
		Value,
		frame::{column::FrameColumn, frame::Frame},
		value_type::ValueType,
	},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::rows { id: int4, note: Option(utf8), count: Option(int2) }");
	t.admin("CREATE PROCEDURE test::add { id: int4, note: Option(utf8), count: Option(int2) } AS { \
		 INSERT test::rows [{ id: $id, note: $note, count: $count }]; map { note: $note, count: $count } }");
	t
}

fn named(entries: Vec<(&str, Value)>) -> Params {
	let map: HashMap<String, Value> = entries.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
	Params::from(map)
}

fn call(t: &TestEngine, rql: &str, params: Params) -> Result<Vec<Frame>, Diagnostic> {
	let r = t.inner().command_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => Err(e.diagnostic()),
		None => Ok(r.frames),
	}
}

fn column<'a>(frames: &'a [Frame], name: &str) -> &'a FrameColumn {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	assert_eq!(frames[0].row_count(), 1, "expected exactly one row in {:?}", frames[0]);
	frames[0].columns.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("column {name} missing"))
}

const ADD: &str = "CALL test::add($id, $note, $count)";

#[test]
fn a_typed_none_binds_as_a_none_of_the_parameter_type() {
	// The body sees the parameter as the declared Option(utf8), so `map { note: $note }` must come back as a
	// none of Utf8 and the inserted row must hold the same typed none.
	let t = engine();

	let frames = call(
		&t,
		ADD,
		named(vec![
			("id", Value::Int4(1)),
			("note", Value::none_of(ValueType::Utf8)),
			("count", Value::none_of(ValueType::Int2)),
		]),
	)
	.expect("typed none into Option(utf8)");

	let note = column(&frames, "note");
	assert_eq!(note.data.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
	assert_eq!(
		note.data.get_value(0),
		Value::None {
			inner: ValueType::Utf8
		}
	);

	let stored = t.query("FROM test::rows");
	assert_eq!(
		column(&stored, "note").data.get_value(0),
		Value::None {
			inner: ValueType::Utf8
		}
	);
	assert_eq!(
		column(&stored, "count").data.get_value(0),
		Value::None {
			inner: ValueType::Int2
		}
	);
}

#[test]
fn an_untyped_none_binds_as_a_none_of_the_parameter_type() {
	// A Rust caller can still send Value::none(); binding must type it instead of leaking an Option(Any)
	// variable into the body.
	let t = engine();

	let frames =
		call(&t, ADD, named(vec![("id", Value::Int4(1)), ("note", Value::none()), ("count", Value::none())]))
			.expect("untyped none into Option(utf8)");

	let note = column(&frames, "note");
	assert_eq!(note.data.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
	assert_eq!(
		note.data.get_value(0),
		Value::None {
			inner: ValueType::Utf8
		}
	);
	assert_eq!(
		column(&frames, "count").data.get_value(0),
		Value::None {
			inner: ValueType::Int2
		}
	);
}

#[test]
fn a_value_binds_as_the_parameter_type_and_is_stored() {
	let t = engine();

	let frames = call(
		&t,
		ADD,
		named(vec![
			("id", Value::Int4(1)),
			("note", Value::Utf8("hello".to_string())),
			("count", Value::Int2(3)),
		]),
	)
	.expect("value into Option(utf8)");

	assert_eq!(column(&frames, "note").data.get_value(0), Value::Utf8("hello".to_string()));
	assert_eq!(column(&frames, "count").data.get_value(0), Value::Int2(3));

	let stored = t.query("FROM test::rows");
	assert_eq!(column(&stored, "note").data.get_value(0), Value::Utf8("hello".to_string()));
	assert_eq!(column(&stored, "count").data.get_value(0), Value::Int2(3));
}

#[test]
fn a_wrong_typed_argument_is_a_cast_error_naming_the_parameter() {
	// The argument is coerced before the body runs, so the failure must point at the parameter, not at the
	// insert inside the body, and nothing may have been inserted.
	let t = engine();

	let err = call(
		&t,
		ADD,
		named(vec![("id", Value::Int4(1)), ("note", Value::none()), ("count", Value::Utf8("abc".to_string()))]),
	)
	.expect_err("utf8 into Option(int2) must fail");

	assert_eq!(err.code, "CAST_002", "{err:?}");
	assert_eq!(err.fragment.text(), "count", "the cast error must name the parameter: {err:?}");
	assert_eq!(TestEngine::row_count(&t.query("FROM test::rows")), 0);
}

#[test]
fn too_few_arguments_is_an_arity_error() {
	// Zipping short used to leave the trailing parameters unbound; the call must fail up front and name
	// the procedure and both counts.
	let t = engine();

	let err = call(&t, "CALL test::add(1, $note)", named(vec![("note", Value::none())]))
		.expect_err("two arguments for three parameters must fail");

	assert_eq!(err.code, "FUNCTION_002", "{err:?}");
	assert_eq!(err.message, "procedure test::add expects 3 arguments, got 2");
	assert_eq!(TestEngine::row_count(&t.query("FROM test::rows")), 0);
}

#[test]
fn too_many_arguments_is_an_arity_error() {
	// Extra arguments used to be dropped silently.
	let t = engine();

	let err = call(&t, "CALL test::add(1, 'x', 2, 4)", Params::None)
		.expect_err("four arguments for three parameters must fail");

	assert_eq!(err.code, "FUNCTION_002", "{err:?}");
	assert_eq!(err.message, "procedure test::add expects 3 arguments, got 4");
	assert_eq!(TestEngine::row_count(&t.query("FROM test::rows")), 0);
}

#[test]
fn a_binding_call_binds_named_request_params_by_name_and_coerces_them() {
	// The server executes a bound procedure as `CALL ns::proc()` with the arguments as named request params;
	// they must reach the body typed exactly like positional arguments do.
	let t = engine();

	let frames = call(
		&t,
		"CALL test::add()",
		named(vec![("id", Value::Int4(1)), ("note", Value::none()), ("count", Value::Int2(7))]),
	)
	.expect("named params bound by name");

	let note = column(&frames, "note");
	assert_eq!(note.data.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
	assert_eq!(
		note.data.get_value(0),
		Value::None {
			inner: ValueType::Utf8
		}
	);
	assert_eq!(column(&frames, "count").data.get_value(0), Value::Int2(7));

	let stored = t.query("FROM test::rows");
	assert_eq!(
		column(&stored, "note").data.get_value(0),
		Value::None {
			inner: ValueType::Utf8
		}
	);
	assert_eq!(column(&stored, "count").data.get_value(0), Value::Int2(7));
}

#[test]
fn a_binding_call_missing_a_named_param_is_an_arity_error() {
	let t = engine();

	let err = call(&t, "CALL test::add()", named(vec![("id", Value::Int4(1)), ("note", Value::none())]))
		.expect_err("a missing named param must fail");

	assert_eq!(err.code, "FUNCTION_002", "{err:?}");
	assert_eq!(err.message, "procedure test::add expects 3 arguments, got 2 named");
	assert_eq!(TestEngine::row_count(&t.query("FROM test::rows")), 0);
}

fn parity_engine() -> TestEngine {
	// One procedure per side of each pair: the same body, differing only in whether the parameter is T or
	// Option(T), so any divergence between the two sides is the parameter type's doing.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::text_plain { v: utf8 }");
	t.admin("CREATE TABLE test::text_opt { v: Option(utf8) }");
	t.admin(
		"CREATE PROCEDURE test::text_plain { v: utf8 } AS { INSERT test::text_plain [{ v: $v }]; map { v: $v } }",
	);
	t.admin(
		"CREATE PROCEDURE test::text_opt { v: Option(utf8) } AS { INSERT test::text_opt [{ v: $v }]; map { v: $v } }",
	);
	t.admin("CREATE PROCEDURE test::int2_plain { v: int2 } AS { map { v: $v } }");
	t.admin("CREATE PROCEDURE test::int2_opt { v: Option(int2) } AS { map { v: $v } }");
	t.admin("CREATE PROCEDURE test::int4_plain { v: int4 } AS { map { v: $v } }");
	t.admin("CREATE PROCEDURE test::int4_opt { v: Option(int4) } AS { map { v: $v } }");
	t
}

#[test]
fn int4_into_utf8_and_into_option_utf8_both_bind_the_text() {
	// Option(T) must behave exactly like T: the cast path stringifies a number into a text parameter on both
	// sides, and the row lands in the plain column as Utf8 and in the optional column as Option(Utf8).
	let t = parity_engine();

	let plain = call(&t, "CALL test::text_plain($v)", named(vec![("v", Value::Int4(42))])).expect("int4 into utf8");
	let opt = call(&t, "CALL test::text_opt($v)", named(vec![("v", Value::Int4(42))]))
		.expect("int4 into Option(utf8)");

	assert_eq!(column(&plain, "v").data.get_value(0), Value::Utf8("42".to_string()));
	assert_eq!(column(&opt, "v").data.get_value(0), column(&plain, "v").data.get_value(0));

	let stored_plain = column(&t.query("FROM test::text_plain"), "v").data.clone();
	let stored_opt = column(&t.query("FROM test::text_opt"), "v").data.clone();
	assert_eq!(stored_plain.get_value(0), Value::Utf8("42".to_string()));
	assert_eq!(stored_opt.get_value(0), stored_plain.get_value(0));
	assert_eq!(stored_plain.get_type(), ValueType::Utf8);
	assert_eq!(stored_opt.get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
}

#[test]
fn text_into_int2_and_into_option_int2_both_fail_with_the_same_cast_error() {
	let t = parity_engine();

	let plain = call(&t, "CALL test::int2_plain($v)", named(vec![("v", Value::Utf8("abc".to_string()))]))
		.expect_err("utf8 into int2 must fail");
	let opt = call(&t, "CALL test::int2_opt($v)", named(vec![("v", Value::Utf8("abc".to_string()))]))
		.expect_err("utf8 into Option(int2) must fail");

	assert_eq!(plain.code, "CAST_002", "{plain:?}");
	assert_eq!(opt.code, plain.code, "{opt:?}");
	assert_eq!(plain.fragment.text(), "v");
	assert_eq!(opt.fragment.text(), plain.fragment.text());
	let plain_cause = plain.cause.as_ref().expect("plain cast error carries its cause");
	let opt_cause = opt.cause.as_ref().expect("optional cast error carries its cause");
	assert_eq!(plain_cause.code, "NUMBER_001", "{plain:?}");
	assert_eq!(opt_cause.code, plain_cause.code, "{opt:?}");
	assert_eq!(opt_cause.label, plain_cause.label);
	assert_eq!(opt.message, plain.message);
}

#[test]
fn none_into_int4_and_into_option_int4_both_bind_a_none_of_int4() {
	let t = parity_engine();

	let plain = call(&t, "CALL test::int4_plain($v)", named(vec![("v", Value::none())])).expect("none into int4");
	let opt =
		call(&t, "CALL test::int4_opt($v)", named(vec![("v", Value::none())])).expect("none into Option(int4)");

	assert_eq!(
		column(&plain, "v").data.get_value(0),
		Value::None {
			inner: ValueType::Int4
		}
	);
	assert_eq!(column(&opt, "v").data.get_value(0), column(&plain, "v").data.get_value(0));
	assert_eq!(column(&plain, "v").data.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
	assert_eq!(column(&opt, "v").data.get_type(), column(&plain, "v").data.get_type());
}
