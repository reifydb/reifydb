// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, frame::data::FrameColumnData, value_type::ValueType},
};

fn x_with_nested_none_param(rql: &str) -> FrameColumnData {
	let t = TestEngine::new();
	let param = Value::none_of(ValueType::Option(Box::new(ValueType::Int4)));
	let params = Params::Named(Arc::new(HashMap::from([("v".to_string(), param)])));
	let result = t.inner().query_as(TestEngine::identity(), rql, params);
	if let Some(err) = result.error {
		panic!("the query must succeed, got {err:?}\nrql: {rql}");
	}
	assert_eq!(result.frames.len(), 1, "expected exactly one frame");
	result.frames[0].columns.iter().find(|c| c.name == "x").expect("column x").data.clone()
}

#[test]
fn a_none_param_of_a_nullable_type_reads_back_as_one_none() {
	// A client may send a none typed Option(int4); using it in RQL must give a none, never a server panic.
	let x = x_with_nested_none_param("MAP { x: $v }");
	assert_eq!(x.get_type(), ValueType::Option(Box::new(ValueType::Int4)), "exactly one option layer");
	assert_eq!(x.get_value(0), Value::none_of(ValueType::Int4));
}

#[test]
fn a_none_param_of_a_nullable_type_reads_back_as_a_none_on_every_row() {
	// Broadcasting the param over several rows must give a none per row, never a server panic.
	let x = x_with_nested_none_param("FROM [{a: 1}, {a: 2}] | MAP { x: $v }");
	assert_eq!(x.get_type(), ValueType::Option(Box::new(ValueType::Int4)), "exactly one option layer");
	assert_eq!(
		(0..x.len()).map(|row| x.get_value(row)).collect::<Vec<_>>(),
		vec![Value::none_of(ValueType::Int4), Value::none_of(ValueType::Int4)]
	);
}
