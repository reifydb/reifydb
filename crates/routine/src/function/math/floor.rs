// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{decimal, float4_with_bitvec, float8_with_bitvec, rename};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::decimal_array::decimals,
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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Float4(container) => {
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
				float4_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Float8(container) => {
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
				float8_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Decimal(container) => {
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
				decimal(ctx.fragment.text(), precision, scale, data)
			}
			_ if data.get_type().is_number() => rename(args[0].clone(), ctx.fragment.text()),
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
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
