// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::{ValueType, input_types::InputTypes};

use crate::function::support::numeric::numeric_to_f64;

pub struct Sign {
	info: RoutineInfo,
}

impl Default for Sign {
	fn default() -> Self {
		Self::new()
	}
}

impl Sign {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sign"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Sign {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		if !data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: data.get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut res_bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match numeric_to_f64(data, i) {
				Some(v) => {
					let sign = if v > 0.0 {
						1i32
					} else if v < 0.0 {
						-1i32
					} else {
						0i32
					};
					result.push(sign);
					res_bitvec.push(true);
				}
				None => {
					result.push(0);
					res_bitvec.push(false);
				}
			}
		}

		let result_data = ColumnBuffer::int4_with_bitvec(result, res_bitvec);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for Sign {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
