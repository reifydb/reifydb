// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod http;
mod ws;

use std::{collections::HashMap, sync::Arc};

use reifydb_client::{Frame, Params, Value, ValueType};

pub fn option(inner: ValueType) -> ValueType {
	ValueType::Option(Box::new(inner))
}

pub fn positional(value: Value) -> Params {
	Params::Positional(Arc::new(vec![value]))
}

pub fn named(value: Value) -> Params {
	Params::Named(Arc::new(HashMap::from([("v".to_string(), value)])))
}

/// The echoed column must come back with the parameter's type, not just a value that
/// prints the same, so the type is asserted separately from the single row.
pub fn assert_echoed(frames: &[Frame], expected_type: ValueType, expected: Value) {
	assert_eq!(frames.len(), 1, "expected one frame, got {}", frames.len());
	let column = &frames[0].columns[0];
	assert_eq!(column.name, "v");
	assert_eq!(column.data.len(), 1);
	assert_eq!(column.data.get_type(), expected_type);
	assert_eq!(column.data.get_value(0), expected);
}
