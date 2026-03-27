// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{Value, r#type::Type};

use crate::function::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct JsonObject;

impl JsonObject {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for JsonObject {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() % 2 != 0 {
			return Err(ScalarFunctionError::ExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "json::object requires an even number of arguments (key-value pairs)"
					.to_string(),
			});
		}

		// Validate that key columns (even indices) are Utf8
		for i in (0..columns.len()).step_by(2) {
			let col = columns.get(i).unwrap();
			match col.data() {
				ColumnData::Utf8 {
					..
				} => {}
				other => {
					return Err(ScalarFunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: i,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let num_pairs = columns.len() / 2;
		let mut results: Vec<Box<Value>> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut fields = Vec::with_capacity(num_pairs);
			for pair in 0..num_pairs {
				let key_col = columns.get(pair * 2).unwrap();
				let val_col = columns.get(pair * 2 + 1).unwrap();

				let key: String = key_col.data().get_as::<String>(row).unwrap_or_default();
				let value = val_col.data().get_value(row);

				fields.push((key, value));
			}
			results.push(Box::new(Value::Record(fields)));
		}

		Ok(ColumnData::any(results))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}
}
