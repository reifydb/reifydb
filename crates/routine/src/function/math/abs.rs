// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::sign::Signed;
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, int::Int, r#type::Type, uint::Uint};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Abs {
	info: FunctionInfo,
}

impl Default for Abs {
	fn default() -> Self {
		Self::new()
	}
}

impl Abs {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::abs"),
		}
	}
}

impl Function for Abs {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.data().unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnData::Int1(container) => {
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
				ColumnData::int1_with_bitvec(data, res_bitvec)
			}
			ColumnData::Int2(container) => {
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
				ColumnData::int2_with_bitvec(data, res_bitvec)
			}
			ColumnData::Int4(container) => {
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
				ColumnData::int4_with_bitvec(data, res_bitvec)
			}
			ColumnData::Int8(container) => {
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
				ColumnData::int8_with_bitvec(data, res_bitvec)
			}
			ColumnData::Int16(container) => {
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
				ColumnData::int16_with_bitvec(data, res_bitvec)
			}
			ColumnData::Uint1(container) => {
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
				ColumnData::uint1_with_bitvec(data, res_bitvec)
			}
			ColumnData::Uint2(container) => {
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
				ColumnData::uint2_with_bitvec(data, res_bitvec)
			}
			ColumnData::Uint4(container) => {
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
				ColumnData::uint4_with_bitvec(data, res_bitvec)
			}
			ColumnData::Uint8(container) => {
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
				ColumnData::uint8_with_bitvec(data, res_bitvec)
			}
			ColumnData::Uint16(container) => {
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
				ColumnData::uint16_with_bitvec(data, res_bitvec)
			}
			ColumnData::Float4(container) => {
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
				ColumnData::float4_with_bitvec(data, res_bitvec)
			}
			ColumnData::Float8(container) => {
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
				ColumnData::float8_with_bitvec(data, res_bitvec)
			}
			ColumnData::Int {
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
				ColumnData::Int {
					container: NumberContainer::new(data),
					max_bytes: *max_bytes,
				}
			}
			ColumnData::Uint {
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
				ColumnData::Uint {
					container: NumberContainer::new(data),
					max_bytes: *max_bytes,
				}
			}
			ColumnData::Decimal {
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
				ColumnData::Decimal {
					container: NumberContainer::new(data),
					precision: *precision,
					scale: *scale,
				}
			}
			other => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![
						Type::Int1,
						Type::Int2,
						Type::Int4,
						Type::Int8,
						Type::Int16,
						Type::Uint1,
						Type::Uint2,
						Type::Uint4,
						Type::Uint8,
						Type::Uint16,
						Type::Float4,
						Type::Float8,
						Type::Int,
						Type::Uint,
						Type::Decimal,
					],
					actual: other.get_type(),
				});
			}
		};

		let final_data = if let Some(bv) = bitvec {
			ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
