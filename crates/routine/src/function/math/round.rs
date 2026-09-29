// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_buffer::i256;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{decimal, float4_with_bitvec, float8_with_bitvec, rename};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::{precision::Precision, scale::Scale},
	container::decimal_array::decimals,
	decimal::{Decimal, unscaled},
	value_type::{ValueType, input_types::InputTypes},
};

use crate::function::support::coerce::read_i32;

pub struct Round {
	info: RoutineInfo,
}

impl Default for Round {
	fn default() -> Self {
		Self::new()
	}
}

impl Round {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::round"),
		}
	}
}

fn round_decimal(value: &Decimal, digits: i32, precision: Precision, scale: Scale) -> Option<Decimal> {
	let scale = scale.value();
	let drop = (scale as i32).saturating_sub(digits);
	if drop <= 0 {
		return value.fits(precision.value(), scale);
	}
	let drop = drop.min(unscaled::MAX_DIGITS as i32 + 1) as u8;
	let rounded = unscaled::round_half_up(value.unscaled(), drop);
	let unscaled = if rounded == i256::ZERO {
		i256::ZERO
	} else {
		unscaled::upscale(rounded, drop)?
	};
	Decimal::from_parts(unscaled, scale)?.fits(precision.value(), scale)
}

impl<'a> Routine<FunctionContext<'a>> for Round {
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
		let val_data = ColumnView::try_from(&args[0])?;
		let precision_column = args.get(1).map(ColumnView::try_from).transpose()?;

		let row_count = val_data.len();

		if let Some(prec_col) = &precision_column
			&& !prec_col.get_type().is_integer()
		{
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: prec_col.get_type(),
			});
		}

		let fragment = ctx.fragment.clone();
		let get_precision = |row_idx: usize| -> Result<i32, RoutineError> {
			match &precision_column {
				Some(prec_col) => Ok(read_i32(&fragment, prec_col, row_idx)?.unwrap_or(0)),
				None => Ok(0),
			}
		};

		let result_data = match &val_data.data {
			ViewData::Float4(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						let precision = get_precision(i)?;
						let multiplier = 10_f32.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}
				float4_with_bitvec(ctx.fragment.text(), result, bitvec)
			}
			ViewData::Float8(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						let precision = get_precision(i)?;
						let multiplier = 10_f64.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}
				float8_with_bitvec(ctx.fragment.text(), result, bitvec)
			}
			ViewData::Decimal(container) => {
				let precision = container.precision();
				let scale = container.scale();
				let mut result = Vec::with_capacity(row_count);
				for (i, value) in decimals(container).into_iter().enumerate() {
					let rounded = round_decimal(&value, get_precision(i)?, precision, scale)
						.ok_or_else(|| RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"round of {value} is out of range for {}",
								ValueType::decimal(precision, scale)
							),
						})?;
					result.push(rounded);
				}
				decimal(ctx.fragment.text(), precision, scale, result)
			}
			_ if val_data.get_type().is_number() => rename(args[0].clone(), ctx.fragment.text()),
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: val_data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for Round {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::AtLeast(1)
	}
}
