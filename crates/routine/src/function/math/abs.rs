// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{
	decimal, float4_with_bitvec, float8_with_bitvec, int1_with_bitvec, int2_with_bitvec, int4_with_bitvec,
	int8_with_bitvec, int16_with_bitvec, uint1_with_bitvec, uint2_with_bitvec, uint4_with_bitvec,
	uint8_with_bitvec, uint16_with_bitvec,
};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{decimal_array::decimals, wide_int_array::wide_at},
	decimal::Decimal,
	value_type::ValueType,
};

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub struct Abs {
	info: RoutineInfo,
}

impl Default for Abs {
	fn default() -> Self {
		Self::new()
	}
}

impl Abs {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::abs"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Abs {
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
		let column_type = data.get_type();

		let result_data = match &data.data {
			ViewData::Int1(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.checked_abs().ok_or_else(|| {
							failed(
								ctx,
								format!(
									"the absolute value of {value} is out of range for {column_type}"
								),
							)
						})?);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				int1_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Int2(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.checked_abs().ok_or_else(|| {
							failed(
								ctx,
								format!(
									"the absolute value of {value} is out of range for {column_type}"
								),
							)
						})?);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				int2_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Int4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.checked_abs().ok_or_else(|| {
							failed(
								ctx,
								format!(
									"the absolute value of {value} is out of range for {column_type}"
								),
							)
						})?);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				int4_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Int8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.checked_abs().ok_or_else(|| {
							failed(
								ctx,
								format!(
									"the absolute value of {value} is out of range for {column_type}"
								),
							)
						})?);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				int8_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Int16(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = wide_at::<i128>(container, i) {
						data.push(value.checked_abs().ok_or_else(|| {
							failed(
								ctx,
								format!(
									"the absolute value of {value} is out of range for {column_type}"
								),
							)
						})?);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				int16_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Uint1(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				uint1_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Uint2(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				uint2_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Uint4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				uint4_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Uint8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				uint8_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Uint16(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = wide_at::<u128>(container, i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				uint16_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Float4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.values().get(i) {
						data.push(value.abs());
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
						data.push(value.abs());
						res_bitvec.push(true);
					} else {
						data.push(0.0);
						res_bitvec.push(false);
					}
				}
				float8_with_bitvec(ctx.fragment.text(), data, res_bitvec)
			}
			ViewData::Decimal(container) => decimal(
				ctx.fragment.text(),
				container.precision(),
				container.scale(),
				decimals(container).iter().map(Decimal::abs),
			),
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
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
						ValueType::Float4,
						ValueType::Float8,
						ValueType::DECIMAL,
					],
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for Abs {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
