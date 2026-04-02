// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{Value, r#type::Type};

use crate::function::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionResult, propagate_options};

pub struct JsonArray;

impl Default for JsonArray {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonArray {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for JsonArray {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		let mut results: Vec<Box<Value>> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut items = Vec::with_capacity(columns.len());
			for col in columns.iter() {
				items.push(col.data().get_value(row));
			}
			results.push(Box::new(Value::List(items)));
		}

		Ok(ColumnData::any(results))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}
}
