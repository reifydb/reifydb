// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	error::Diagnostic,
	params::Params,
	value::{Value, frame::frame::Frame},
};

fn named(entries: Vec<(&str, Value)>) -> Params {
	let map: HashMap<String, Value> = entries.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
	Params::from(map)
}

fn admin(t: &TestEngine, rql: &str) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn command(t: &TestEngine, rql: &str, params: Params) -> Result<Vec<Frame>, Box<Diagnostic>> {
	let r = t.inner().command_as(TestEngine::identity(), rql, params);
	match r.error {
		Some(e) => Err(e.0),
		None => Ok(r.frames),
	}
}

fn create_procedure_error(param_type: &str) -> Diagnostic {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE app");
	*admin(&t, &format!("CREATE PROCEDURE app::p {{ ids: {param_type} }} AS {{ map {{ one: 1 }} }}"))
		.expect_err("the procedure must be refused")
}

fn add_all() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE app");
	t.admin("CREATE TABLE app::t { id: int4 }");
	t.admin(
		"CREATE PROCEDURE app::add_all { ids: list(int4) } AS { for $id in $ids { insert app::t [{ id: $id }] } }",
	);
	t
}

fn stored_ids(t: &TestEngine) -> Vec<i32> {
	let frames = t.query("FROM app::t");
	let mut ids: Vec<i32> = frames[0].rows().map(|r| r.get::<i32>("id").unwrap().unwrap()).collect();
	ids.sort();
	ids
}

#[test]
fn a_bare_list_param_type_reports_the_missing_item_type() {
	// Without an item type the engine cannot cast the items, so a bare list must be refused at create time.
	let err = create_procedure_error("list");
	assert_eq!(err.code, "AST_021", "{err:?}");
}

#[test]
fn a_list_with_two_item_types_reports_the_extra_parameter() {
	// A second parameter has no meaning, so accepting it would silently ignore what the author wrote.
	let err = create_procedure_error("list(int4, int4)");
	assert_eq!(err.code, "AST_022", "{err:?}");
}

#[test]
fn a_list_item_that_is_not_a_type_is_refused() {
	// A literal where the item type belongs must not be read as a type.
	let err = create_procedure_error("list(5)");
	assert_eq!(err.code, "AST_023", "{err:?}");
}

#[test]
fn a_nested_list_reports_a_non_scalar_item() {
	// FOR splits the list into one row per item, so a list item must be a scalar, never another list.
	let err = create_procedure_error("list(list(int4))");
	assert_eq!(err.code, "AST_024", "{err:?}");
}

#[test]
fn a_constrained_list_item_is_refused_instead_of_dropping_the_constraint() {
	// The list type keeps only the item type, so utf8(10) would lose its limit without a word.
	let err = create_procedure_error("list(utf8(10))");
	assert_eq!(err.code, "AST_025", "{err:?}");
}

#[test]
fn a_list_column_is_still_refused() {
	// list(T) is allowed on procedure params only; a list column must not slip in through the same resolver.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE app");
	let err = admin(&t, "CREATE TABLE app::t { ids: list(int4) }").expect_err("a list column must be refused");
	assert_eq!(err.code, "AST_008", "{err:?}");
}

#[test]
fn a_list_argument_with_a_bad_item_fails_the_cast_and_inserts_nothing() {
	// Only item 0 decides the list's reported type, so every item must be cast or a bad one reaches the body.
	let t = add_all();

	let err = command(
		&t,
		"CALL app::add_all($ids)",
		named(vec![("ids", Value::List(vec![Value::Int4(1), Value::Utf8("abc".to_string())]))]),
	)
	.expect_err("utf8 item into list(int4) must fail");

	assert_eq!(err.code, "CAST_002", "{err:?}");
	assert_eq!(err.fragment.text(), "ids", "the cast error must name the parameter: {err:?}");
	assert_eq!(TestEngine::row_count(&t.query("FROM app::t")), 0);
}

#[test]
fn int_literals_in_a_list_argument_are_cast_to_the_item_type() {
	// RQL int literals are not int4, so without a per-item cast a literal list could never be passed.
	let t = add_all();

	command(&t, "CALL app::add_all([10, 20, 30])", Params::None).expect("a literal int list must bind");

	assert_eq!(stored_ids(&t), vec![10, 20, 30]);
}

#[test]
fn a_for_loop_over_a_list_of_plain_values_runs_once_per_item() {
	// A list left as one row runs the loop once with the whole list as the loop value.
	let t = TestEngine::new();

	let frames = command(
		&t,
		"let $n = 0; for $v in $ids { $n = $n + 1 }; map { n: $n }",
		named(vec![("ids", Value::List(vec![Value::Int4(10), Value::Int4(20), Value::Int4(30)]))]),
	)
	.expect("a loop over a list param must run");

	assert_eq!(frames.len(), 1, "{frames:?}");
	let n = frames[0].columns.iter().find(|c| c.name == "n").expect("column n");
	assert_eq!(n.data.get_value(0).to_string(), "3", "{frames:?}");
}
