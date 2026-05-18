// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{
	Value, container::any::AnyContainer, frame::data::FrameColumnData, ordered_f64::OrderedF64, r#type::Type,
};

fn make(v: Vec<Value>) -> FrameColumnData {
	FrameColumnData::Any(AnyContainer::new(v.into_iter().map(Box::new).collect()))
}

crate::nones_tests! {
	values: vec![
		Value::Int4(42),
		Value::Utf8("hello".to_string()),
		Value::Boolean(true),
		Value::Float8(OrderedF64::try_from(3.14).unwrap()),
		Value::Int8(i64::MIN),
	],
	inner_type: Type::Any,
}
