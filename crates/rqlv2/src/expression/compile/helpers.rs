// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Helper utilities for expression compilation.

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_type::{fragment::Fragment, util::bitvec::BitVec, value::Value};

use crate::expression::types::{EvalError, EvalResult};

/// Broadcast a scalar value to a column with the given row count.
pub(super) fn broadcast_value(value: &Value, row_count: usize) -> EvalResult<Column> {
	let data = match value {
		Value::Undefined => ColumnData::undefined(row_count),
		Value::Boolean(v) => ColumnData::bool(vec![*v; row_count]),
		Value::Int8(v) => ColumnData::int8(vec![*v; row_count]),
		Value::Float8(v) => ColumnData::float8(std::iter::repeat(f64::from(*v)).take(row_count)),
		Value::Utf8(s) => ColumnData::utf8(std::iter::repeat(s.clone()).take(row_count).collect::<Vec<_>>()),
		_ => {
			return Err(EvalError::UnsupportedOperation {
				operation: format!("broadcast of value type {:?}", value),
			});
		}
	};

	Ok(Column::new(Fragment::internal("_var"), data))
}

/// Convert a boolean column to a BitVec mask.
pub(super) fn column_to_mask(column: &Column) -> EvalResult<BitVec> {
	match column.data() {
		ColumnData::Bool(container) => {
			let mask = BitVec::from_fn(container.len(), |i| container.get(i).unwrap_or(false));
			Ok(mask)
		}
		other => Err(EvalError::TypeMismatch {
			expected: "boolean".to_string(),
			found: format!("{:?}", other.get_type()),
			context: "filter predicate".to_string(),
		}),
	}
}
