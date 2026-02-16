// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	value::{container::number::NumberContainer, decimal::Decimal, r#type::Type},
};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Power;

impl Power {
	pub fn new() -> Self {
		Self {}
	}
}

fn convert_column_to_type(data: &ColumnData, target: Type, row_count: usize) -> ColumnData {
	match target {
		Type::Int1 => {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					let val = get_as_i8(data, i);
					result.push(val);
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
					let val = get_as_i16(data, i);
					result.push(val);
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
					let val = get_as_i32(data, i);
					result.push(val);
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
					let val = get_as_i64(data, i);
					result.push(val);
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
					let val = get_as_i128(data, i);
					result.push(val);
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
					let val = get_as_u8(data, i);
					result.push(val);
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
					let val = get_as_u16(data, i);
					result.push(val);
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
					let val = get_as_u32(data, i);
					result.push(val);
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
					let val = get_as_u64(data, i);
					result.push(val);
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
					let val = get_as_u128(data, i);
					result.push(val);
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
					let val = get_as_f32(data, i);
					result.push(val);
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
					let val = get_as_f64(data, i);
					result.push(val);
					bitvec.push(true);
				} else {
					result.push(0.0);
					bitvec.push(false);
				}
			}
			ColumnData::float8_with_bitvec(result, bitvec)
		}
		Type::Int => {
			use reifydb_type::value::int::Int;
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					let val = get_as_i128(data, i);
					result.push(Int::from(val));
					bitvec.push(true);
				} else {
					result.push(Int::default());
					bitvec.push(false);
				}
			}
			ColumnData::int_with_bitvec(result, bitvec)
		}
		Type::Uint => {
			use reifydb_type::value::uint::Uint;
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					let val = get_as_u128(data, i);
					result.push(Uint::from(val));
					bitvec.push(true);
				} else {
					result.push(Uint::default());
					bitvec.push(false);
				}
			}
			ColumnData::uint_with_bitvec(result, bitvec)
		}
		Type::Decimal => {
			use reifydb_type::value::decimal::Decimal;
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					let val = get_as_f64(data, i);
					result.push(Decimal::from(val));
					bitvec.push(true);
				} else {
					result.push(Decimal::default());
					bitvec.push(false);
				}
			}
			ColumnData::decimal_with_bitvec(result, bitvec)
		}
		// For same type or unsupported conversions, clone the original
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

/// Promotes two numeric types to a common type for power computation.
/// This handles types that the standard Type::promote doesn't handle well,
/// including Int, Uint, and Decimal.
fn promote_numeric_types(left: Type, right: Type) -> Type {
	use Type::*;

	if matches!(left, Float4 | Float8 | Decimal) || matches!(right, Float4 | Float8 | Decimal) {
		return Decimal;
	}

	// If any type is the arbitrary-precision Int or Uint, promote to the largest fixed type
	// Int -> Int16 (largest signed), Uint -> Uint16 (largest unsigned)
	// But if mixing signed/unsigned, go to Int16
	if left == Int || right == Int {
		return Int16;
	}
	if left == Uint || right == Uint {
		// If the other type is signed, go to Int16
		if matches!(left, Int1 | Int2 | Int4 | Int8 | Int16)
			|| matches!(right, Int1 | Int2 | Int4 | Int8 | Int16)
		{
			return Int16;
		}
		return Uint16;
	}

	// For standard fixed-size types, use the standard promotion logic
	Type::promote(left, right)
}

impl ScalarFunction for Power {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate exactly 2 arguments
		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let base_column = columns.get(0).unwrap();
		let exponent_column = columns.get(1).unwrap();

		match (base_column.data(), exponent_column.data()) {
			(ColumnData::Int1(base_container), ColumnData::Int1(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								(base_val as i32).pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Int2(base_container), ColumnData::Int2(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								(base_val as i32).pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Int4(base_container), ColumnData::Int4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Int8(base_container), ColumnData::Int8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Int16(base_container), ColumnData::Int16(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Uint1(base_container), ColumnData::Uint1(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result =
								(base_val as u32).saturating_pow(exp_val as u32);
							result.push(power_result);
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
			(ColumnData::Uint2(base_container), ColumnData::Uint2(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result =
								(base_val as u32).saturating_pow(exp_val as u32);
							result.push(power_result);
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
			(ColumnData::Uint4(base_container), ColumnData::Uint4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val);
							result.push(power_result);
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
			(ColumnData::Uint8(base_container), ColumnData::Uint8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val as u32);
							result.push(power_result);
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
			(ColumnData::Uint16(base_container), ColumnData::Uint16(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val as u32);
							result.push(power_result);
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
			(ColumnData::Float4(base_container), ColumnData::Float4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							result.push(base_val.powf(exp_val));
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
			(ColumnData::Float8(base_container), ColumnData::Float8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							result.push(base_val.powf(exp_val));
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
					container: base_container,
					max_bytes,
				},
				ColumnData::Int {
					container: exp_container,
					..
				},
			) => {
				use reifydb_type::value::int::Int;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);
							result.push(Int::from(b.powf(e) as i64));
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
					container: base_container,
					max_bytes,
				},
				ColumnData::Uint {
					container: exp_container,
					..
				},
			) => {
				use reifydb_type::value::uint::Uint;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);
							result.push(Uint::from(b.powf(e) as u64));
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
					container: base_container,
					precision,
					scale,
				},
				ColumnData::Decimal {
					container: exp_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);

							result.push(Decimal::from(b.powf(e)));
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
			// Mixed-type case: promote both columns to a common type and recurse
			(base_data, exp_data) => {
				let base_type = base_data.get_type();
				let exp_type = exp_data.get_type();

				if !base_type.is_number() || !exp_type.is_number() {
					return Err(ScalarFunctionError::InvalidArgumentType {
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
						actual: base_type,
					});
				}

				let promoted_type = promote_numeric_types(base_type, exp_type);

				let promoted_base = convert_column_to_type(base_data, promoted_type.clone(), row_count);
				let promoted_exp = convert_column_to_type(exp_data, promoted_type, row_count);

				let base_col = Column::new(Fragment::internal("base"), promoted_base);
				let exp_col = Column::new(Fragment::internal("exp"), promoted_exp);
				let promoted_columns = Columns::new(vec![base_col, exp_col]);

				let new_ctx = ScalarFunctionContext {
					fragment: ctx.fragment.clone(),
					columns: &promoted_columns,
					row_count,
					clock: ctx.clock,
				};
				self.scalar(new_ctx)
			}
		}
	}
}
