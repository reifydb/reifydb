// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::decimal_array::{decimal_array, decimals},
	decimal::Decimal,
	value_type::{ValueType, input_types::InputTypes},
};

pub struct Floor {
	info: RoutineInfo,
}

impl Default for Floor {
	fn default() -> Self {
		Self::new()
	}
}

impl Floor {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::floor"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Floor {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().cloned().unwrap_or(ValueType::Float8)
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Float4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.floor());
						res_bitvec.push(true);
					} else {
						data.push(0.0);
						res_bitvec.push(false);
					}
				}
				ColumnBuffer::float4_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Float8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.floor());
						res_bitvec.push(true);
					} else {
						data.push(0.0);
						res_bitvec.push(false);
					}
				}
				ColumnBuffer::float8_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Decimal(container) => {
				let precision = container.precision();
				let scale = container.scale();
				let mut data = Vec::with_capacity(row_count);
				for value in decimals(container) {
					let rounded = Decimal::from_parts(value.floor(), 0)
						.and_then(|rounded| rounded.fits(precision.value(), scale.value()))
						.ok_or_else(|| RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"floor of {value} is out of range for {}",
								ValueType::decimal(precision, scale)
							),
						})?;
					data.push(rounded);
				}
				ColumnBuffer::Decimal(decimal_array(precision, scale, data))
			}
			other if other.get_type().is_number() => data.clone(),
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for Floor {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
