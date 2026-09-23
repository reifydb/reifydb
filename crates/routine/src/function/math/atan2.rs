// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::{ValueType, input_types::InputTypes};

use crate::function::support::numeric::numeric_to_f64;

pub struct Atan2 {
	info: RoutineInfo,
}

impl Default for Atan2 {
	fn default() -> Self {
		Self::new()
	}
}

impl Atan2 {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::atan2"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Atan2 {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Float8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let y_data = &args[0];
		let x_data = &args[1];
		let row_count = y_data.len();

		if !y_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: y_data.get_type(),
			});
		}

		if !x_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: x_data.get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut res_bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match (numeric_to_f64(y_data, i), numeric_to_f64(x_data, i)) {
				(Some(y), Some(x)) => {
					result.push(y.atan2(x));
					res_bitvec.push(true);
				}
				_ => {
					result.push(0.0);
					res_bitvec.push(false);
				}
			}
		}

		let result_data = ColumnBuffer::float8_with_bitvec(result, res_bitvec);

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for Atan2 {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
