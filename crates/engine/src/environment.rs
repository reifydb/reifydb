// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, ColumnData, Columns};
use reifydb_type::{Fragment, Value};

pub fn create_env_columns() -> Columns {
	let mut keys = Vec::new();
	let mut values = Vec::new();

	keys.push("version");
	values.push(Box::new(Value::Utf8("0.0.1".to_string())));

	keys.push("answer");
	values.push(Box::new(Value::uint1(42)));

	let name_column = Column {
		name: Fragment::internal("key".to_string()),
		data: ColumnData::utf8(keys),
	};

	let value_column = Column {
		name: Fragment::internal("value".to_string()),
		data: ColumnData::any(values),
	};

	Columns::new(vec![name_column, value_column])
}
