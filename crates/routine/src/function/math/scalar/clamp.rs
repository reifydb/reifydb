// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	value::{container::number::NumberContainer, decimal::Decimal, int::Int, r#type::Type, uint::Uint},
};

use crate::function::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct Clamp;

impl Default for Clamp {
	fn default() -> Self {
		Self::new()
	}
}

impl Clamp {
	pub fn new() -> Self {
		Self
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

impl ScalarFunction for Clamp {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 3 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: columns.len(),
			});
		}

		let val_col = columns.first().unwrap();
		let min_col = columns.get(1).unwrap();
		let max_col = columns.get(2).unwrap();

		match (val_col.data(), min_col.data(), max_col.data()) {
			(ColumnData::Int1(v), ColumnData::Int1(lo), ColumnData::Int1(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::int1_with_bitvec(result, bitvec))
			}
			(ColumnData::Int2(v), ColumnData::Int2(lo), ColumnData::Int2(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::int2_with_bitvec(result, bitvec))
			}
			(ColumnData::Int4(v), ColumnData::Int4(lo), ColumnData::Int4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			(ColumnData::Int8(v), ColumnData::Int8(lo), ColumnData::Int8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::int8_with_bitvec(result, bitvec))
			}
			(ColumnData::Int16(v), ColumnData::Int16(lo), ColumnData::Int16(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::int16_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint1(v), ColumnData::Uint1(lo), ColumnData::Uint1(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::uint1_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint2(v), ColumnData::Uint2(lo), ColumnData::Uint2(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::uint2_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint4(v), ColumnData::Uint4(lo), ColumnData::Uint4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint8(v), ColumnData::Uint8(lo), ColumnData::Uint8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::uint8_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint16(v), ColumnData::Uint16(lo), ColumnData::Uint16(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::uint16_with_bitvec(result, bitvec))
			}
			(ColumnData::Float4(v), ColumnData::Float4(lo), ColumnData::Float4(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::float4_with_bitvec(result, bitvec))
			}
			(ColumnData::Float8(v), ColumnData::Float8(lo), ColumnData::Float8(hi)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v.get(i), lo.get(i), hi.get(i)) {
						(Some(&val), Some(&min), Some(&max)) => {
							result.push(val.clamp(min, max));
							bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::float8_with_bitvec(result, bitvec))
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
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Int::from(v.clamp(l, h) as i64));
							bitvec.push(true);
						}
						_ => {
							result.push(Int::default());
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::Int {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
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
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Uint::from(v.clamp(l, h) as u64));
							bitvec.push(true);
						}
						_ => {
							result.push(Uint::default());
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::Uint {
					container: NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
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
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					match (v_container.get(i), lo_container.get(i), hi_container.get(i)) {
						(Some(val), Some(lo), Some(hi)) => {
							let v = val.0.to_f64().unwrap_or(0.0);
							let l = lo.0.to_f64().unwrap_or(0.0);
							let h = hi.0.to_f64().unwrap_or(0.0);
							result.push(Decimal::from(v.clamp(l, h)));
							bitvec.push(true);
						}
						_ => {
							result.push(Decimal::default());
							bitvec.push(false);
						}
					}
				}
				Ok(ColumnData::Decimal {
					container: NumberContainer::new(result),
					precision: *precision,
					scale: *scale,
				})
			}
			// Mixed-type fallback: validate all 3 are numeric, promote to common type and recurse
			(v_data, lo_data, hi_data) => {
				let v_type = v_data.get_type();
				let lo_type = lo_data.get_type();
				let hi_type = hi_data.get_type();

				let numeric_types = vec![
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
				];

				for (idx, t) in [(0, &v_type), (1, &lo_type), (2, &hi_type)] {
					if !t.is_number() {
						return Err(ScalarFunctionError::InvalidArgumentType {
							function: ctx.fragment.clone(),
							argument_index: idx,
							expected: numeric_types.clone(),
							actual: t.clone(),
						});
					}
				}

				let promoted = promote_numeric_types(v_type, lo_type, hi_type);
				let pv = convert_column_to_type(v_data, promoted.clone(), row_count);
				let plo = convert_column_to_type(lo_data, promoted.clone(), row_count);
				let phi = convert_column_to_type(hi_data, promoted, row_count);

				let val_col = Column::new(Fragment::internal("val"), pv);
				let min_col = Column::new(Fragment::internal("min"), plo);
				let max_col = Column::new(Fragment::internal("max"), phi);
				let promoted_columns = Columns::new(vec![val_col, min_col, max_col]);

				let new_ctx = ScalarFunctionContext {
					fragment: ctx.fragment.clone(),
					columns: &promoted_columns,
					row_count,
					runtime_context: ctx.runtime_context,
					identity: ctx.identity,
				};
				self.scalar(new_ctx)
			}
		}
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
}
