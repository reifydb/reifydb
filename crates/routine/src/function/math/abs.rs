// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_traits::sign::Signed;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::{
		bignum_array::{decimal_array, decimal_at, int_array, int_at, uint_array, uint_at},
		decimal_array::u128_at,
	},
	decimal::Decimal,
	int::Int,
	uint::Uint,
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

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();
		let column_type = data.get_type();

		let result_data = match data {
			ColumnBuffer::Int1(container) => {
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
				ColumnBuffer::int1_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Int2(container) => {
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
				ColumnBuffer::int2_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Int4(container) => {
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
				ColumnBuffer::int4_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Int8(container) => {
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
				ColumnBuffer::int8_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Int16(container) => {
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
				ColumnBuffer::int16_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Uint1(container) => {
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
				ColumnBuffer::uint1_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Uint2(container) => {
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
				ColumnBuffer::uint2_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Uint4(container) => {
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
				ColumnBuffer::uint4_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Uint8(container) => {
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
				ColumnBuffer::uint8_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Uint16(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = u128_at(container, i) {
						data.push(value);
						res_bitvec.push(true);
					} else {
						data.push(0);
						res_bitvec.push(false);
					}
				}
				ColumnBuffer::uint16_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Float4(container) => {
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
				ColumnBuffer::float4_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Float8(container) => {
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
				ColumnBuffer::float8_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Int {
				container,
				max_bytes,
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = int_at(container, i) {
						data.push(Int::from(value.0.abs()));
					} else {
						data.push(Int::default());
					}
				}
				ColumnBuffer::Int {
					container: int_array(data),
					max_bytes: *max_bytes,
				}
			}
			ColumnBuffer::Uint {
				container,
				max_bytes,
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = uint_at(container, i) {
						data.push(value);
					} else {
						data.push(Uint::default());
					}
				}
				ColumnBuffer::Uint {
					container: uint_array(data),
					max_bytes: *max_bytes,
				}
			}
			ColumnBuffer::Decimal {
				container,
				precision,
				scale,
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = decimal_at(container, i) {
						data.push(Decimal::from(value.0.abs()));
					} else {
						data.push(Decimal::default());
					}
				}
				ColumnBuffer::Decimal {
					container: decimal_array(data),
					precision: *precision,
					scale: *scale,
				}
			}
			other => {
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
						ValueType::Int,
						ValueType::Uint,
						ValueType::Decimal,
					],
					actual: other.get_type(),
				});
			}
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
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
