// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{Value, value_type::ValueType};

pub struct JsonArray {
	info: RoutineInfo,
}

impl Default for JsonArray {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonArray {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("json::array"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for JsonArray {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::any(vec![Value::List(vec![])]),
			)]));
		}

		let row_count = args[0].len();
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut items = Vec::with_capacity(args.len());
			for col in args.iter() {
				items.push(col.data().get_value(row));
			}
			results.push(Value::List(items));
		}

		let result_data = ColumnBuffer::any(results);

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for JsonArray {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Any
	}
}
