// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{interface::change::Change, row::Row};
use reifydb_testing_sdk::builders::{TestChangeBuilder, TestRowBuilder};
use reifydb_value::value::{Value, datetime::DateTime, row_number::RowNumber, value_type::ValueType};

pub fn shape() -> RowShape {
	RowShape::new(
		RowFamily::Deprecated,
		vec![
			RowShapeField::unconstrained("g".to_string(), ValueType::Int4),
			RowShapeField::unconstrained("v".to_string(), ValueType::Int8),
		],
	)
}

pub fn row(number: RowNumber, group: i32, value: i64, at: DateTime) -> Row {
	let mut row = TestRowBuilder::new(number)
		.with_shape(shape())
		.with_values(vec![Value::Int4(group), Value::Int8(value)])
		.build();
	row.encoded.set_timestamps(at, at);
	row.encoded.set_time(at);
	row
}

pub fn insert(rows: Vec<Row>) -> Change {
	let mut builder = TestChangeBuilder::new();
	for row in rows {
		builder = builder.insert(row);
	}
	builder.build()
}

pub fn remove(rows: Vec<Row>) -> Change {
	let mut builder = TestChangeBuilder::new();
	for row in rows {
		builder = builder.remove(row);
	}
	builder.build()
}

pub fn update(pairs: Vec<(Row, Row)>) -> Change {
	let mut builder = TestChangeBuilder::new();
	for (pre, post) in pairs {
		builder = builder.update(pre, post);
	}
	builder.build()
}
