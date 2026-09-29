// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::{
	batch::batch,
	column::factory::{any, utf8},
};
use reifydb_value::value::Value;

use crate::Result;

pub fn create_env_columns() -> Result<RecordBatch> {
	let mut keys = Vec::new();
	let mut values = Vec::new();

	keys.push("version");
	values.push(Value::Utf8("0.0.1".to_string()));

	keys.push("answer");
	values.push(Value::uint1(42));

	let name_column = utf8("key", keys);

	let value_column = any("value", values);

	batch(vec![name_column, value_column])
}
