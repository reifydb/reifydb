// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use num_traits::sign::Signed;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::{
	container::number::NumberContainer, decimal::Decimal, int::Int, uint::Uint, value_type::ValueType,
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

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
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Int1(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(&value) = container.get(i) {
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
					if let Some(value) = container.get(i) {
						data.push(Int::from(value.0.clone().abs()));
					} else {
						data.push(Int::default());
					}
				}
				ColumnBuffer::Int {
					container: NumberContainer::new(data),
					max_bytes: *max_bytes,
				}
			}
			ColumnBuffer::Uint {
				container,
				max_bytes,
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(value.clone());
					} else {
						data.push(Uint::default());
					}
				}
				ColumnBuffer::Uint {
					container: NumberContainer::new(data),
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
					if let Some(value) = container.get(i) {
						data.push(Decimal::from(value.0.clone().abs()));
					} else {
						data.push(Decimal::default());
					}
				}
				ColumnBuffer::Decimal {
					container: NumberContainer::new(data),
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

		let final_data = if let Some(bv) = bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for Abs {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
