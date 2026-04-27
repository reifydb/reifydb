// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{fragment::Fragment, value::Value};

pub fn create_env_columns() -> Columns {
	let mut keys = Vec::new();
	let mut values = Vec::new();

	keys.push("version");
	values.push(Box::new(Value::Utf8("0.0.1".to_string())));

	keys.push("answer");
	values.push(Box::new(Value::uint1(42)));

	let name_column = ColumnWithName::new(Fragment::internal("key"), ColumnBuffer::utf8(keys));

	let value_column = ColumnWithName::new(Fragment::internal("value"), ColumnBuffer::any(values));

	Columns::new(vec![name_column, value_column])
}
