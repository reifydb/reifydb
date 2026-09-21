// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

fn regions() -> Value {
	Value::List(vec![
		Value::Record(vec![
			("id".to_string(), Value::Utf8("us".to_string())),
			("label".to_string(), Value::Utf8("US".to_string())),
		]),
		Value::Record(vec![
			("id".to_string(), Value::Utf8("eu".to_string())),
			("label".to_string(), Value::Utf8("EU".to_string())),
		]),
	])
}

fn named(name: &str, value: Value) -> Params {
	Params::Named(Arc::new(HashMap::from([(name.to_string(), value)])))
}

#[test]
fn a_list_of_records_param_inserts_one_row_per_element() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::monitor_regions { id: utf8, label: utf8 }");

	t.command_with_params("INSERT test::monitor_regions $regions", named("regions", regions()));

	let frames = t.query("FROM test::monitor_regions");
	assert_eq!(TestEngine::row_count(&frames), 2);

	let mut values: Vec<_> = frames[0]
		.rows()
		.map(|r| (r.get::<String>("id").unwrap().unwrap(), r.get::<String>("label").unwrap().unwrap()))
		.collect();
	values.sort();
	assert_eq!(values, vec![("eu".to_string(), "EU".to_string()), ("us".to_string(), "US".to_string())]);
}

#[test]
fn an_empty_list_of_records_param_inserts_no_rows() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::monitor_regions { id: utf8, label: utf8 }");

	t.command_with_params("INSERT test::monitor_regions $regions", named("regions", Value::List(vec![])));

	let frames = t.query("FROM test::monitor_regions");
	assert_eq!(TestEngine::row_count(&frames), 0);
}

#[test]
fn a_list_of_records_param_with_mismatched_row_shape_errors() {
	let t = TestEngine::new();

	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::monitor_regions { id: utf8, label: utf8 }");

	let mismatched = Value::List(vec![
		Value::Record(vec![
			("id".to_string(), Value::Utf8("us".to_string())),
			("label".to_string(), Value::Utf8("US".to_string())),
		]),
		Value::Record(vec![("id".to_string(), Value::Utf8("eu".to_string()))]),
	]);

	t.command_with_params_err("INSERT test::monitor_regions $regions", named("regions", mismatched));
}
