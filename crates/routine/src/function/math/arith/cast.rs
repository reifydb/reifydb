// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_type::value::{decimal::Decimal, int::Int, number::safe::convert::SafeConvert, r#type::Type, uint::Uint};

pub fn promote_two(left: Type, right: Type) -> Type {
	if left == right {
		return left;
	}
	if left == Type::Decimal || right == Type::Decimal {
		return Type::Decimal;
	}
	if matches!(left, Type::Float4 | Type::Float8) || matches!(right, Type::Float4 | Type::Float8) {
		return Type::Float8;
	}
	if left == Type::Int || right == Type::Int {
		return Type::Int;
	}
	if left == Type::Uint || right == Type::Uint {
		let is_signed_primitive =
			|t: &Type| matches!(t, Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16);
		if is_signed_primitive(&left) || is_signed_primitive(&right) {
			return Type::Int;
		}
		return Type::Uint;
	}

	let rank = |t: &Type| match t {
		Type::Int1 | Type::Uint1 => 0,
		Type::Int2 | Type::Uint2 => 1,
		Type::Int4 | Type::Uint4 => 2,
		Type::Int8 | Type::Uint8 => 3,
		Type::Int16 | Type::Uint16 => 4,
		_ => 0,
	};
	let is_signed = |t: &Type| matches!(t, Type::Int1 | Type::Int2 | Type::Int4 | Type::Int8 | Type::Int16);
	let is_unsigned = |t: &Type| matches!(t, Type::Uint1 | Type::Uint2 | Type::Uint4 | Type::Uint8 | Type::Uint16);

	let max_rank = rank(&left).max(rank(&right));
	if is_signed(&left) && is_signed(&right) {
		return [Type::Int1, Type::Int2, Type::Int4, Type::Int8, Type::Int16][max_rank].clone();
	}
	if is_unsigned(&left) && is_unsigned(&right) {
		return [Type::Uint1, Type::Uint2, Type::Uint4, Type::Uint8, Type::Uint16][max_rank].clone();
	}
	let bumped = (max_rank + 1).min(4);
	[Type::Int1, Type::Int2, Type::Int4, Type::Int8, Type::Int16][bumped].clone()
}

macro_rules! make_extract {
	($fn_name:ident, $T:ty, $default:expr) => {
		pub fn $fn_name(data: &ColumnBuffer, i: usize) -> $T {
			match data {
				ColumnBuffer::Int1(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Int2(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Int4(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Int8(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Int16(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Uint1(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Uint2(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Uint4(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Uint8(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Uint16(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Float4(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Float8(c) => {
					c.get(i).copied().map(|v| v.saturating_convert()).unwrap_or($default)
				}
				ColumnBuffer::Int {
					container,
					..
				} => container.get(i).cloned().map(|v| v.saturating_convert()).unwrap_or($default),
				ColumnBuffer::Uint {
					container,
					..
				} => container.get(i).cloned().map(|v| v.saturating_convert()).unwrap_or($default),
				ColumnBuffer::Decimal {
					container,
					..
				} => container.get(i).cloned().map(|v| v.saturating_convert()).unwrap_or($default),
				_ => $default,
			}
		}
	};
}

make_extract!(get_as_i8, i8, 0i8);
make_extract!(get_as_i16, i16, 0i16);
make_extract!(get_as_i32, i32, 0i32);
make_extract!(get_as_i64, i64, 0i64);
make_extract!(get_as_i128, i128, 0i128);
make_extract!(get_as_u8, u8, 0u8);
make_extract!(get_as_u16, u16, 0u16);
make_extract!(get_as_u32, u32, 0u32);
make_extract!(get_as_u64, u64, 0u64);
make_extract!(get_as_u128, u128, 0u128);
make_extract!(get_as_f32, f32, 0.0f32);
make_extract!(get_as_f64, f64, 0.0f64);
make_extract!(get_as_big_int, Int, Int::zero());
make_extract!(get_as_big_uint, Uint, Uint::zero());
make_extract!(get_as_decimal, Decimal, Decimal::default());

macro_rules! make_convert {
	($fn_name:ident, $T:ty, $factory:ident, $extract:ident, $default:expr) => {
		fn $fn_name(data: &ColumnBuffer, row_count: usize) -> ColumnBuffer {
			let mut result = Vec::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if data.is_defined(i) {
					result.push($extract(data, i));
					bitvec.push(true);
				} else {
					result.push($default);
					bitvec.push(false);
				}
			}
			ColumnBuffer::$factory(result, bitvec)
		}
	};
}

make_convert!(to_int1, i8, int1_with_bitvec, get_as_i8, 0i8);
make_convert!(to_int2, i16, int2_with_bitvec, get_as_i16, 0i16);
make_convert!(to_int4, i32, int4_with_bitvec, get_as_i32, 0i32);
make_convert!(to_int8, i64, int8_with_bitvec, get_as_i64, 0i64);
make_convert!(to_int16, i128, int16_with_bitvec, get_as_i128, 0i128);
make_convert!(to_uint1, u8, uint1_with_bitvec, get_as_u8, 0u8);
make_convert!(to_uint2, u16, uint2_with_bitvec, get_as_u16, 0u16);
make_convert!(to_uint4, u32, uint4_with_bitvec, get_as_u32, 0u32);
make_convert!(to_uint8, u64, uint8_with_bitvec, get_as_u64, 0u64);
make_convert!(to_uint16, u128, uint16_with_bitvec, get_as_u128, 0u128);
make_convert!(to_float4, f32, float4_with_bitvec, get_as_f32, 0.0f32);
make_convert!(to_float8, f64, float8_with_bitvec, get_as_f64, 0.0f64);
make_convert!(to_int, Int, int_with_bitvec, get_as_big_int, Int::zero());
make_convert!(to_uint, Uint, uint_with_bitvec, get_as_big_uint, Uint::zero());
make_convert!(to_decimal, Decimal, decimal_with_bitvec, get_as_decimal, Decimal::default());

pub fn convert_column_to_type(data: &ColumnBuffer, target: Type, row_count: usize) -> ColumnBuffer {
	match target {
		Type::Int1 => to_int1(data, row_count),
		Type::Int2 => to_int2(data, row_count),
		Type::Int4 => to_int4(data, row_count),
		Type::Int8 => to_int8(data, row_count),
		Type::Int16 => to_int16(data, row_count),
		Type::Uint1 => to_uint1(data, row_count),
		Type::Uint2 => to_uint2(data, row_count),
		Type::Uint4 => to_uint4(data, row_count),
		Type::Uint8 => to_uint8(data, row_count),
		Type::Uint16 => to_uint16(data, row_count),
		Type::Float4 => to_float4(data, row_count),
		Type::Float8 => to_float8(data, row_count),
		Type::Int => to_int(data, row_count),
		Type::Uint => to_uint(data, row_count),
		Type::Decimal => to_decimal(data, row_count),
		_ => data.clone(),
	}
}
