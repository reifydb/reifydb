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

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Power {
	info: RoutineInfo,
}

impl Default for Power {
	fn default() -> Self {
		Self::new()
	}
}

impl Power {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::power"),
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

impl<'a> Routine<FunctionContext<'a>> for Power {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		if input_types.len() >= 2 {
			promote_two(input_types[0].clone(), input_types[1].clone())
		} else {
			Type::Float8
		}
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let base_column = &args[0];
		let exponent_column = &args[1];

		let (base_data, base_bv) = base_column.unwrap_option();
		let (exp_data, exp_bv) = exponent_column.unwrap_option();
		let row_count = base_data.len();

		let result_data = match (base_data, exp_data) {
			(ColumnBuffer::Int1(base), ColumnBuffer::Int1(exp)) => {
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
				ColumnBuffer::int4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int2(base), ColumnBuffer::Int2(exp)) => {
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
				ColumnBuffer::int4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int4(base), ColumnBuffer::Int4(exp)) => {
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
				ColumnBuffer::int4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int8(base), ColumnBuffer::Int8(exp)) => {
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
				ColumnBuffer::int8_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Int16(base), ColumnBuffer::Int16(exp)) => {
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
				ColumnBuffer::int16_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Float4(base), ColumnBuffer::Float4(exp)) => {
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
				ColumnBuffer::float4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Float8(base), ColumnBuffer::Float8(exp)) => {
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
				ColumnBuffer::float8_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint1(base), ColumnBuffer::Uint1(exp)) => {
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
				ColumnBuffer::uint4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint2(base), ColumnBuffer::Uint2(exp)) => {
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
				ColumnBuffer::uint4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint4(base), ColumnBuffer::Uint4(exp)) => {
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
				ColumnBuffer::uint4_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint8(base), ColumnBuffer::Uint8(exp)) => {
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
				ColumnBuffer::uint8_with_bitvec(result, res_bitvec)
			}
			(ColumnBuffer::Uint16(base), ColumnBuffer::Uint16(exp)) => {
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
				ColumnBuffer::uint16_with_bitvec(result, res_bitvec)
			}
			(
				ColumnBuffer::Int {
					container: base,
					max_bytes,
				},
				ColumnBuffer::Int {
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
				ColumnBuffer::Int {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnBuffer::Uint {
					container: base,
					max_bytes,
				},
				ColumnBuffer::Uint {
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
				ColumnBuffer::Uint {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				}
			}
			(
				ColumnBuffer::Decimal {
					container: base,
					precision,
					scale,
				},
				ColumnBuffer::Decimal {
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
				ColumnBuffer::Decimal {
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
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.env.fragment.clone(),
						argument_index: 0,
						expected: InputTypes::numeric().expected_at(0).to_vec(),
						actual: base_type,
					});
				}

				let promoted_type = promote_two(base_type, exp_type);
				let promoted_base = convert_column_to_type(base_data, promoted_type.clone(), row_count);
				let promoted_exp = convert_column_to_type(exp_data, promoted_type, row_count);

				let base_col = ColumnWithName::new(Fragment::internal("base"), promoted_base);
				let exp_col = ColumnWithName::new(Fragment::internal("exp"), promoted_exp);
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
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv,
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
	}
}
