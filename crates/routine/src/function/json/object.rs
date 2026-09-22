// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{Value, value_type::ValueType};

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

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.len().is_multiple_of(2) {
			return Err(RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "json::object requires an even number of arguments (key-value pairs)"
					.to_string(),
			});
		}

		for i in (0..args.len()).step_by(2) {
			let col_data = &args[i];
			match col_data {
				ColumnBuffer::Utf8 {
					..
				} => {}
				other => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: i,
						expected: vec![ValueType::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let row_count = if args.is_empty() {
			1
		} else {
			args[0].len()
		};
		let num_pairs = args.len() / 2;
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut fields = Vec::with_capacity(num_pairs);
			for pair in 0..num_pairs {
				let key_data = &args[pair * 2];
				let val_data = &args[pair * 2 + 1];

				let key: String = key_data.get_as::<String>(row).unwrap_or_default();
				let value = val_data.get_value(row);

				fields.push((key, value));
			}
			results.push(Value::Record(fields));
		}

		let result_data = ColumnBuffer::any(results);

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
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
