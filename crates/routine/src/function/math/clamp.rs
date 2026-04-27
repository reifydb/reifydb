// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
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

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct Clamp {
	info: RoutineInfo,
}

impl Default for Clamp {
	fn default() -> Self {
		Self::new()
	}
}

impl Clamp {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::clamp"),
		}
	}
}

fn convert_column_to_type(data: &ColumnBuffer, target: Type, row_count: usize) -> ColumnBuffer {
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
			ColumnBuffer::int1_with_bitvec(result, bitvec)
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
			ColumnBuffer::int2_with_bitvec(result, bitvec)
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
			ColumnBuffer::int4_with_bitvec(result, bitvec)
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
			ColumnBuffer::int8_with_bitvec(result, bitvec)
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
			ColumnBuffer::int16_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint1_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint2_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint4_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint8_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint16_with_bitvec(result, bitvec)
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
			ColumnBuffer::float4_with_bitvec(result, bitvec)
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
			ColumnBuffer::float8_with_bitvec(result, bitvec)
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
			ColumnBuffer::int_with_bitvec(result, bitvec)
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
			ColumnBuffer::uint_with_bitvec(result, bitvec)
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
			ColumnBuffer::decimal_with_bitvec(result, bitvec)
		}
		_ => data.clone(),
	}
}

fn get_as_i8(data: &ColumnBuffer, i: usize) -> i8 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i8).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u8(data: &ColumnBuffer, i: usize) -> u8 {
	match data {
		ColumnBuffer::Uint1(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as u8).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i16(data: &ColumnBuffer, i: usize) -> i16 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i16).unwrap_or(0),
		ColumnBuffer::Int2(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i16).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i32(data: &ColumnBuffer, i: usize) -> i32 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnBuffer::Int4(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i32).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i64(data: &ColumnBuffer, i: usize) -> i64 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnBuffer::Int8(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i64).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_i128(data: &ColumnBuffer, i: usize) -> i128 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Int8(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Int16(c) => c.get(i).copied().unwrap_or(0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as i128).unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u16(data: &ColumnBuffer, i: usize) -> u16 {
	match data {
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as u16).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u32(data: &ColumnBuffer, i: usize) -> u32 {
	match data {
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as u32).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as u32).unwrap_or(0),
		ColumnBuffer::Uint4(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u64(data: &ColumnBuffer, i: usize) -> u64 {
	match data {
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as u64).unwrap_or(0),
		ColumnBuffer::Uint8(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_u128(data: &ColumnBuffer, i: usize) -> u128 {
	match data {
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as u128).unwrap_or(0),
		ColumnBuffer::Uint16(c) => c.get(i).copied().unwrap_or(0),
		_ => 0,
	}
}

fn get_as_f32(data: &ColumnBuffer, i: usize) -> f32 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Int8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Int16(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Uint16(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Float4(c) => c.get(i).copied().unwrap_or(0.0),
		ColumnBuffer::Float8(c) => c.get(i).map(|&v| v as f32).unwrap_or(0.0),
		ColumnBuffer::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnBuffer::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnBuffer::Decimal {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f32().unwrap_or(0.0)).unwrap_or(0.0),
		_ => 0.0,
	}
}

fn get_as_f64(data: &ColumnBuffer, i: usize) -> f64 {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Int8(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Int16(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Uint16(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Float4(c) => c.get(i).map(|&v| v as f64).unwrap_or(0.0),
		ColumnBuffer::Float8(c) => c.get(i).copied().unwrap_or(0.0),
		ColumnBuffer::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnBuffer::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)).unwrap_or(0.0),
		ColumnBuffer::Decimal {
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

impl<'a> Routine<FunctionContext<'a>> for Clamp {
	fn info(&self) -> &RoutineInfo {
		&self.info
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

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 3 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		let val_col = &args[0];
		let min_col = &args[1];
		let max_col = &args[2];

		let (v_data, v_bv) = val_col.unwrap_option();
		let (lo_data, lo_bv) = min_col.unwrap_option();
		let (hi_data, hi_bv) = max_col.unwrap_option();
		let row_count = v_data.len();

		let result_data = match (v_data, lo_data, hi_data) {
			(ColumnBuffer::Int1(v), ColumnBuffer::Int1(lo), ColumnBuffer::Int1(hi)) => {
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
				ColumnBuffer::int1_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int2(v), ColumnBuffer::Int2(lo), ColumnBuffer::Int2(hi)) => {
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
				ColumnBuffer::int2_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int4(v), ColumnBuffer::Int4(lo), ColumnBuffer::Int4(hi)) => {
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
				ColumnBuffer::int4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int8(v), ColumnBuffer::Int8(lo), ColumnBuffer::Int8(hi)) => {
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
				ColumnBuffer::int8_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int16(v), ColumnBuffer::Int16(lo), ColumnBuffer::Int16(hi)) => {
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
				ColumnBuffer::int16_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint1(v), ColumnBuffer::Uint1(lo), ColumnBuffer::Uint1(hi)) => {
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
				ColumnBuffer::uint1_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint2(v), ColumnBuffer::Uint2(lo), ColumnBuffer::Uint2(hi)) => {
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
				ColumnBuffer::uint2_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint4(v), ColumnBuffer::Uint4(lo), ColumnBuffer::Uint4(hi)) => {
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
				ColumnBuffer::uint4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint8(v), ColumnBuffer::Uint8(lo), ColumnBuffer::Uint8(hi)) => {
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
				ColumnBuffer::uint8_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint16(v), ColumnBuffer::Uint16(lo), ColumnBuffer::Uint16(hi)) => {
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
				ColumnBuffer::uint16_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Float4(v), ColumnBuffer::Float4(lo), ColumnBuffer::Float4(hi)) => {
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
				ColumnBuffer::float4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Float8(v), ColumnBuffer::Float8(lo), ColumnBuffer::Float8(hi)) => {
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
				ColumnBuffer::float8_with_bitvec(result, res_bitvec)
			}
			(
				ColumnBuffer::Int {
					container: v_container,
					max_bytes,
				},
				ColumnBuffer::Int {
					container: lo_container,
					..
				},
				ColumnBuffer::Int {
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
				ColumnBuffer::Int {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnBuffer::Uint {
					container: v_container,
					max_bytes,
				},
				ColumnBuffer::Uint {
					container: lo_container,
					..
				},
				ColumnBuffer::Uint {
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
				ColumnBuffer::Uint {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnBuffer::Decimal {
					container: v_container,
					precision,
					scale,
				},
				ColumnBuffer::Decimal {
					container: lo_container,
					..
				},
				ColumnBuffer::Decimal {
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
				ColumnBuffer::Decimal {
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
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 0,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: v_type,
					});
				}
				if !lo_type.is_number() {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: lo_type,
					});
				}
				if !hi_type.is_number() {
					return Err(RoutineError::FunctionInvalidArgumentType {
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

				let val_col = ColumnWithName::new(Fragment::internal("val"), pv);
				let min_col = ColumnWithName::new(Fragment::internal("min"), plo);
				let max_col = ColumnWithName::new(Fragment::internal("max"), phi);
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
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv,
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for Clamp {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
