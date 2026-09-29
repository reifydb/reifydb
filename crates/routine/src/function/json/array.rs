// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::any;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{Value, column_view::ColumnView, value_type::ValueType};

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		if args.is_empty() {
			return Ok(any(ctx.fragment.text(), vec![Value::List(vec![])]));
		}

		let views = args.iter().map(ColumnView::try_from).collect::<Result<Vec<_>, _>>()?;
		let row_count = views[0].len();
		let mut results: Vec<Value> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut items = Vec::with_capacity(args.len());
			for col in views.iter() {
				items.push(col.get_value(row));
			}
			results.push(Value::List(items));
		}

		Ok(any(ctx.fragment.text(), results))
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
