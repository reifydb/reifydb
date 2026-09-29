// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::any;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	value_type::ValueType,
};

pub struct JsonObject {
	info: RoutineInfo,
}

impl Default for JsonObject {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonObject {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("json::object"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for JsonObject {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		if !args.len().is_multiple_of(2) {
			return Err(RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "json::object requires an even number of arguments (key-value pairs)"
					.to_string(),
			});
		}

		let views = args.iter().map(ColumnView::try_from).collect::<Result<Vec<_>, _>>()?;

		for i in (0..views.len()).step_by(2) {
			let col_data = &views[i];
			match &col_data.data {
				ViewData::Utf8 {
					..
				} => {}
				_ => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: i,
						expected: vec![ValueType::Utf8],
						actual: col_data.get_type(),
					});
				}
			}
		}

		let row_count = if views.is_empty() {
			1
		} else {
			views[0].len()
		};
		let num_pairs = args.len() / 2;
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut fields = Vec::with_capacity(num_pairs);
			for pair in 0..num_pairs {
				let key_data = &views[pair * 2];
				let val_data = &views[pair * 2 + 1];

				let key: String = key_data.get_as::<String>(row)?.unwrap_or_default();
				let value = val_data.get_value(row);

				fields.push((key, value));
			}
			results.push(Value::Record(fields));
		}

		Ok(any(ctx.fragment.text(), results))
	}
}

impl Function for JsonObject {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Any
	}
}
