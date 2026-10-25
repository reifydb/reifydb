// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, value_type::ValueType},
};

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::p { k: uint2, v: int4 } WITH { partition: { by: { k } } }");
	t.command("INSERT test::p [{ k: 1, v: 10 }, { k: 2, v: 20 }, { k: 3, v: 30 }, { k: 4, v: 40 }]");
	t
}

fn params(ks: Vec<Value>) -> Params {
	Params::from(HashMap::from([("ks".to_string(), Value::List(ks))]))
}

fn keys(t: &TestEngine, rql: &str, ks: Vec<Value>) -> Vec<u16> {
	let frames = t.command_with_params(rql, params(ks));
	let mut keys: Vec<u16> =
		frames.iter().flat_map(|f| f.rows()).map(|row| row.get::<u16>("k").unwrap().unwrap()).collect();
	keys.sort();
	keys
}

#[test]
fn in_a_list_parameter_keeps_exactly_the_listed_rows() {
	// a list parameter compared as one value would match nothing, or everything once coerced.
	let t = engine();
	let got = keys(&t, "FROM test::p FILTER { k in $ks } MAP { k }", vec![Value::Uint2(1), Value::Uint2(3)]);
	assert_eq!(got, vec![1, 3]);
}

#[test]
fn not_in_a_list_parameter_keeps_every_other_row() {
	// negation must apply to the whole membership test, not to each item.
	let t = engine();
	let got = keys(&t, "FROM test::p FILTER { k not in $ks } MAP { k }", vec![Value::Uint2(1), Value::Uint2(3)]);
	assert_eq!(got, vec![2, 4]);
}

#[test]
fn in_an_empty_list_parameter_keeps_nothing_and_not_in_keeps_everything() {
	// an empty key list must never widen into a match-all filter.
	let t = engine();
	assert_eq!(keys(&t, "FROM test::p FILTER { k in $ks } MAP { k }", vec![]), Vec::<u16>::new());
	assert_eq!(keys(&t, "FROM test::p FILTER { k not in $ks } MAP { k }", vec![]), vec![1, 2, 3, 4]);
}

#[test]
fn list_items_of_a_wider_integer_type_still_match() {
	// clients send plain integers, so an int8 list must still find uint2 keys.
	let t = engine();
	let got = keys(&t, "FROM test::p FILTER { k in $ks } MAP { k }", vec![Value::Int8(2), Value::Int8(4)]);
	assert_eq!(got, vec![2, 4]);
}

#[test]
fn a_none_item_only_matches_through_the_other_items() {
	// a none item must not turn a miss into a match, it can only make the miss unknown.
	let t = engine();
	let none = Value::None {
		inner: ValueType::Uint2,
	};
	assert_eq!(
		keys(&t, "FROM test::p FILTER { k in $ks } MAP { k }", vec![Value::Uint2(2), none.clone()]),
		vec![2]
	);
	assert_eq!(
		keys(&t, "FROM test::p FILTER { k not in $ks } MAP { k }", vec![Value::Uint2(2), none]),
		Vec::<u16>::new()
	);
}

#[test]
fn a_list_bound_by_let_works_like_a_parameter() {
	// a procedure derives its key list with let, so a let-bound list must behave like a list parameter.
	let t = engine();
	let got = keys(&t, "let $own = $ks; FROM test::p FILTER { k in $own } MAP { k }", vec![Value::Uint2(4)]);
	assert_eq!(got, vec![4]);
}

#[test]
fn a_list_of_an_incomparable_type_is_refused() {
	// silently coercing text to a key would compare garbage and match nothing without telling anyone.
	let t = engine();
	let err = t.command_with_params_err(
		"FROM test::p FILTER { k in $ks } MAP { k }",
		params(vec![Value::Utf8("1".to_string())]),
	);
	assert!(
		err.contains("OPERATOR") && err.contains("Utf8"),
		"expected a type error naming the text item, got: {err}"
	);
}

#[test]
fn update_with_in_a_list_parameter_changes_only_the_listed_rows() {
	// this is the shape place::commit uses, so an update must touch exactly the listed keys.
	let t = engine();
	t.command_with_params(
		"UPDATE test::p { v: 0 } FILTER { k in $ks }",
		params(vec![Value::Uint2(2), Value::Uint2(3)]),
	);
	let frames = t.query("FROM test::p MAP { k, v }");
	let mut rows: Vec<(u16, i32)> = frames
		.iter()
		.flat_map(|f| f.rows())
		.map(|row| (row.get::<u16>("k").unwrap().unwrap(), row.get::<i32>("v").unwrap().unwrap()))
		.collect();
	rows.sort();
	assert_eq!(rows, vec![(1, 10), (2, 0), (3, 0), (4, 40)]);
}
