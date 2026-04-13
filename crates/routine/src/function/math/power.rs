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

pub struct Power {
	info: FunctionInfo,
}

impl Default for Power {
	fn default() -> Self {
		Self::new()
	}
}

impl Power {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::power"),
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
	// Known-side-wins symmetric: (X, Any) and (Any, X) yield X when X is numeric;
	// (Any, Any) yields Any. Runs BEFORE canonicalization so `power(Int, none)`
	// preserves `Int` rather than falling into the Int→Int16 overflow-guard
	// branch (no value exists to overflow when the exponent is null).
	if matches!(left, Type::Any) && matches!(right, Type::Any) {
		return Type::Any;
	}
	if matches!(left, Type::Any) && right.is_number() {
		return right;
	}
	if left.is_number() && matches!(right, Type::Any) {
		return left;
	}
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

impl Function for Power {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		if input_types.len() >= 2 {
			promote_two(input_types[0].clone(), input_types[1].clone())
		} else {
			Type::Float8
		}
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let base_column = &args[0];
		let exponent_column = &args[1];

		let (base_data, base_bv) = base_column.data().unwrap_option();
		let (exp_data, exp_bv) = exponent_column.data().unwrap_option();
		let row_count = base_data.len();

		let result_data = match (base_data, exp_data) {
			(ColumnData::Int1(base), ColumnData::Int1(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(if e < 0 {
								0
							} else {
								(b as i32).pow(e as u32)
							});
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
			(ColumnData::Int2(base), ColumnData::Int2(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(if e < 0 {
								0
							} else {
								(b as i32).pow(e as u32)
							});
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
			(ColumnData::Int4(base), ColumnData::Int4(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(if e < 0 {
								0
							} else {
								b.saturating_pow(e as u32)
							});
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
			(ColumnData::Int8(base), ColumnData::Int8(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(if e < 0 {
								0
							} else {
								b.saturating_pow(e as u32)
							});
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
			(ColumnData::Int16(base), ColumnData::Int16(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(if e < 0 {
								0
							} else {
								b.saturating_pow(e as u32)
							});
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
			(ColumnData::Float4(base), ColumnData::Float4(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(b.powf(e));
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
			(ColumnData::Float8(base), ColumnData::Float8(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(b.powf(e));
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
			(ColumnData::Uint1(base), ColumnData::Uint1(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push((b as u32).saturating_pow(e as u32));
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
			(ColumnData::Uint2(base), ColumnData::Uint2(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push((b as u32).saturating_pow(e as u32));
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
			(ColumnData::Uint4(base), ColumnData::Uint4(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(b.saturating_pow(e));
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
			(ColumnData::Uint8(base), ColumnData::Uint8(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(b.saturating_pow(e as u32));
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
			(ColumnData::Uint16(base), ColumnData::Uint16(exp)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(&b), Some(&e)) => {
							result.push(b.saturating_pow(e as u32));
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
			(
				ColumnData::Int {
					container: base,
					max_bytes,
				},
				ColumnData::Int {
					container: exp,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(b), Some(e)) => {
							let b_val = b.0.to_f64().unwrap_or(0.0);
							let e_val = e.0.to_f64().unwrap_or(0.0);
							result.push(Int::from(b_val.powf(e_val) as i64));
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
					container: base,
					max_bytes,
				},
				ColumnData::Uint {
					container: exp,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(b), Some(e)) => {
							let b_val = b.0.to_f64().unwrap_or(0.0);
							let e_val = e.0.to_f64().unwrap_or(0.0);
							result.push(Uint::from(b_val.powf(e_val) as u64));
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
					container: base,
					precision,
					scale,
				},
				ColumnData::Decimal {
					container: exp,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (base.get(i), exp.get(i)) {
						(Some(b), Some(e)) => {
							let b_val = b.0.to_f64().unwrap_or(0.0);
							let e_val = e.0.to_f64().unwrap_or(0.0);
							result.push(Decimal::from(b_val.powf(e_val)));
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
			// Mixed-type case: promote both columns to a common type and recurse
			_ => {
				let base_type = base_data.get_type();
				let exp_type = exp_data.get_type();

				if !base_type.is_number() || !exp_type.is_number() {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 0,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: base_type,
					});
				}

				let promoted_type = promote_two(base_type, exp_type);
				let promoted_base = convert_column_to_type(base_data, promoted_type.clone(), row_count);
				let promoted_exp = convert_column_to_type(exp_data, promoted_type, row_count);

				let base_col = Column::new(Fragment::internal("base"), promoted_base);
				let exp_col = Column::new(Fragment::internal("exp"), promoted_exp);
				let promoted_columns = Columns::new(vec![base_col, exp_col]);

				return self.call(ctx, &promoted_columns);
			}
		};

		let combined_bitvec = match (base_bv, exp_bv) {
			(Some(b), Some(e)) => Some(b.and(e)),
			(Some(b), None) => Some(b.clone()),
			(None, Some(e)) => Some(e.clone()),
			(None, None) => None,
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
