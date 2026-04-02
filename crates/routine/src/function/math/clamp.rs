// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	value::{
		container::number::NumberContainer,
		decimal::Decimal,
		int::Int,
		r#type::{Type, input_types::InputTypes},
		uint::Uint,
	},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Clamp {
	info: FunctionInfo,
}

impl Default for Clamp {
	fn default() -> Self {
		Self::new()
	}
}

impl Clamp {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::clamp"),
		}
	}
}

fn convert_column_to_type(data: &ColumnData, target: Type, row_count: usize) -> ColumnData {
	match target {
		Type::Int1 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_i8(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::int1_with_bitvec(result, bitvec)
		}
		Type::Int2 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_i16(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::int2_with_bitvec(result, bitvec)
		}
		Type::Int4 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_i32(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::int4_with_bitvec(result, bitvec)
		}
		Type::Int8 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_i64(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::int8_with_bitvec(result, bitvec)
		}
		Type::Int16 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_i128(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::int16_with_bitvec(result, bitvec)
		}
		Type::Uint1 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_u8(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::uint1_with_bitvec(result, bitvec)
		}
		Type::Uint2 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_u16(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::uint2_with_bitvec(result, bitvec)
		}
		Type::Uint4 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_u32(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::uint4_with_bitvec(result, bitvec)
		}
		Type::Uint8 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_u64(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::uint8_with_bitvec(result, bitvec)
		}
		Type::Uint16 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_u128(data, i));
					bitvec.push(true);
				} else {
					result.push(0);
					bitvec.push(false);
				}
			}
			ColumnData::uint16_with_bitvec(result, bitvec)
		}
		Type::Float4 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_f32(data, i));
					bitvec.push(true);
				} else {
					result.push(0.0);
					bitvec.push(false);
				}
			}
			ColumnData::float4_with_bitvec(result, bitvec)
		}
		Type::Float8 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(get_as_f64(data, i));
					bitvec.push(true);
				} else {
					result.push(0.0);
					bitvec.push(false);
				}
			}
			ColumnData::float8_with_bitvec(result, bitvec)
		}
		Type::Int => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(Int::from(get_as_i128(data, i)));
					bitvec.push(true);
				} else {
					result.push(Int::default());
					bitvec.push(false);
				}
			}
			ColumnData::int_with_bitvec(result, bitvec)
		}
		Type::Uint => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(Uint::from(get_as_u128(data, i)));
					bitvec.push(true);
				} else {
					result.push(Uint::default());
					bitvec.push(false);
				}
			}
			ColumnData::uint_with_bitvec(result, bitvec)
		}
		Type::Decimal => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push(Decimal::from(get_as_f64(data, i)));
					bitvec.push(true);
				} else {
					result.push(Decimal::default());
					bitvec.push(false);
				}
			}
			ColumnData::decimal_with_bitvec(result, bitvec)
		}
		_ => data.clone(),
	}
}

fn get_as_i8(data: &ColumnData, i: usize) -> i8 {
	match data {
		ColumnData::Int1(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i8).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u8(data: &ColumnData, i: usize) -> u8 {
	match data {
		ColumnData::Uint1(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Int1(c) => c.get(i).map(|&v| v as u8).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i16(data: &ColumnData, i: usize) -> i16 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i16).unwrap_or(0),
		ColumnData::Int2(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i16).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i32(data: &ColumnData, i: usize) -> i32 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnData::Int4(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i64(data: &ColumnData, i: usize) -> i64 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnData::Int8(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i128(data: &ColumnData, i: usize) -> i128 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Int8(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Int16(c) => c.get(i).copied().unwrap_or(0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u16(data: &ColumnData, i: usize) -> u16 {
	match data {
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as u16).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u32(data: &ColumnData, i: usize) -> u32 {
	match data {
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as u32).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as u32).unwrap_or(0),
		ColumnData::Uint4(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u64(data: &ColumnData, i: usize) -> u64 {
	match data {
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnData::Uint8(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u128(data: &ColumnData, i: usize) -> u128 {
	match data {
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnData::Uint16(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_f32(data: &ColumnData, i: usize) -> f32 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Int8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Float4(c) => c.get(i).copied().unwrap_or(0.0),
		ColumnData::Float8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnData::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnData::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnData::Decimal {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		_ => 0.0,
	}
}

fn get_as_f64(data: &ColumnData, i: usize) -> f64 {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Int8(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Float4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnData::Float8(c) => c.get(i).copied().unwrap_or(0.0),
		ColumnData::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnData::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnData::Decimal {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
		_ => 0.0,
	}
}

fn promote_two(left: Type, right: Type) -> Type {
	if matches!(left, Type::Float4 | Type::Float8 | Type::Decimal)
		|| matches!(right, Type::Float4 | Type::Float8 | Type::Decimal)
	{
		return Type::Decimal;
	}
	if left == Type::Int || right == Type::Int {
		return Type::Int16;
	}
	if left == Type::Uint || right == Type::Uint {
		if matches!(left, Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16)
			|| matches!(right, Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16)
		{
			return Type::Int16;
		}
		return Type::Uint16;
	}
	Type::promote(left, right)
}

fn promote_numeric_types(a: Type, b: Type, c: Type) -> Type {
	promote_two(promote_two(a, b), c)
}

impl Function for Clamp {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		if input_types.len() >= 3
			&& input_types[0].is_number()
			&& input_types[1].is_number()
			&& input_types[2].is_number()
		{
			promote_numeric_types(input_types[0].clone(), input_types[1].clone(), input_types[2].clone())
		} else {
			Type::Float8
		}
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 3 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let val_col = &args[0];
		let min_col = &args[1];
		let max_col = &args[2];

		let (v_data, v_bv) = val_col.data().unwrap_option();
		let (lo_data, lo_bv) = min_col.data().unwrap_option();
		let (hi_data, hi_bv) = max_col.data().unwrap_option();
		let row_count = v_data.len();

		let result_data = match (v_data, lo_data, hi_data) {
			(ColumnData::Int1(v), ColumnData::Int1(lo), ColumnData::Int1(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::int1_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Int2(v), ColumnData::Int2(lo), ColumnData::Int2(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::int2_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Int4(v), ColumnData::Int4(lo), ColumnData::Int4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::int4_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Int8(v), ColumnData::Int8(lo), ColumnData::Int8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::int8_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Int16(v), ColumnData::Int16(lo), ColumnData::Int16(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::int16_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Uint1(v), ColumnData::Uint1(lo), ColumnData::Uint1(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::uint1_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Uint2(v), ColumnData::Uint2(lo), ColumnData::Uint2(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::uint2_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Uint4(v), ColumnData::Uint4(lo), ColumnData::Uint4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::uint4_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Uint8(v), ColumnData::Uint8(lo), ColumnData::Uint8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::uint8_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Uint16(v), ColumnData::Uint16(lo), ColumnData::Uint16(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::uint16_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Float4(v), ColumnData::Float4(lo), ColumnData::Float4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::float4_with_bitvec(result, res_bitvec)
			}
			(ColumnData::Float8(v), ColumnData::Float8(lo), ColumnData::Float8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							res_bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::float8_with_bitvec(result, res_bitvec)
			}
			(
				ColumnData::Int {
					container: v_container,
					max_bytes,
				},
				ColumnData::Int {
					container: lo_container,
					..
				},
				ColumnData::Int {
					container: hi_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Int::from(v.clamp(l, h) as i64));
							res_bitvec.push(true);
						}
						_ => {
							result.push(Int::default());
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::Int {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnData::Uint {
					container: v_container,
					max_bytes,
				},
				ColumnData::Uint {
					container: lo_container,
					..
				},
				ColumnData::Uint {
					container: hi_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Uint::from(v.clamp(l, h) as u64));
							res_bitvec.push(true);
						}
						_ => {
							result.push(Uint::default());
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::Uint {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnData::Decimal {
					container: v_container,
					precision,
					scale,
				},
				ColumnData::Decimal {
					container: lo_container,
					..
				},
				ColumnData::Decimal {
					container: hi_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Decimal::from(v.clamp(l, h)));
							res_bitvec.push(true);
						}
						_ => {
							result.push(Decimal::default());
							res_bitvec.push(false);
						}
					}
				}
				ColumnData::Decimal {
					container: NumberContainer::new(result),
					precision: *precision,
					scale: *scale,
				}
			}
			// Mixed-type fallback: promote all to Decimal or recursion
			_ => {
				let v_type = v_data.get_type();
				let lo_type = lo_data.get_type();
				let hi_type = hi_data.get_type();

				if !v_type.is_number() {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 0,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: v_type,
					});
				}
				if !lo_type.is_number() {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: lo_type,
					});
				}
				if !hi_type.is_number() {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 2,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: hi_type,
					});
				}

				let promoted = promote_numeric_types(v_type, lo_type, hi_type);
				let pv = convert_column_to_type(v_data, promoted.clone(), row_count);
				let plo = convert_column_to_type(lo_data, promoted.clone(), row_count);
				let phi = convert_column_to_type(hi_data, promoted, row_count);

				let val_col = Column::new(Fragment::internal("val"), pv);
				let min_col = Column::new(Fragment::internal("min"), plo);
				let max_col = Column::new(Fragment::internal("max"), phi);
				let promoted_columns = Columns::new(vec![val_col, min_col, max_col]);

				return self.call(ctx, &promoted_columns);
			}
		};

		let combined_bitvec = match (v_bv, lo_bv, hi_bv) {
			(Some(v), Some(lo), Some(hi)) => Some(v.and(lo).and(hi)),
			(Some(v), Some(lo), None) => Some(v.and(lo)),
			(Some(v), None, Some(hi)) => Some(v.and(hi)),
			(None, Some(lo), Some(hi)) => Some(lo.and(hi)),
			(Some(v), None, None) => Some(v.clone()),
			(None, Some(lo), None) => Some(lo.clone()),
			(None, None, Some(hi)) => Some(hi.clone()),
			(None, None, None) => None,
		};

		let final_data = if let Some(bv) = combined_bitvec {
			ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv,
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
