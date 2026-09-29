// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::PI;

use reifydb_value::value::{Value, container::any_array::any_array, ordered_f64::OrderedF64, value_type::ValueType};

use crate::common::{ColumnData, data};

fn make(v: Vec<Value>) -> ColumnData {
	data(ValueType::Any, any_array(v))
}

crate::plain_tests! {
	typical: vec![
		Value::Int4(42),
		Value::Utf8("hello".to_string()),
		Value::Boolean(true),
		Value::Float8(OrderedF64::try_from(PI).unwrap()),
	],
	boundary: vec![
		Value::Int8(i64::MAX),
		Value::Int8(i64::MIN),
		Value::Uint8(u64::MAX),
	],
	single: Value::Int4(1),
}
