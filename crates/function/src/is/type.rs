// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{Value, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError};

pub struct IsType;

impl IsType {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for IsType {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let value_column = columns.get(0).unwrap();
		let type_column = columns.get(1).unwrap();

		// Extract target Type from second arg
		// - ColumnData::Any containing Value::Type → use that type
		// - Value::Undefined → check for Type::Undefined
		let target_type = match type_column.data().get_value(0) {
			Value::Any(boxed) => match boxed.as_ref() {
				Value::Type(t) => *t,
				_ => {
					return Err(ScalarFunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![Type::Any],
						actual: boxed.get_type(),
					});
				}
			},
			Value::Undefined => Type::Undefined,
			other => {
				return Err(ScalarFunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Any],
					actual: other.get_type(),
				});
			}
		};

		// Per-row type check
		let data: Vec<bool> =
			(0..row_count).map(|i| value_column.data().get_value(i).get_type() == target_type).collect();

		Ok(ColumnData::bool(data))
	}
}
