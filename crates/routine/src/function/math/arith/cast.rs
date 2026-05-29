// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	decimal::Decimal, int::Int, number::safe::convert::SafeConvert, uint::Uint, value_type::ValueType,
};

pub fn promote_two(left: ValueType, right: ValueType) -> ValueType {
	if left == right {
		return left;
	}
	if left == ValueType::Decimal || right == ValueType::Decimal {
		return ValueType::Decimal;
	}
	if matches!(left, ValueType::Float4 | ValueType::Float8)
		|| matches!(right, ValueType::Float4 | ValueType::Float8)
	{
		return ValueType::Float8;
	}
	if left == ValueType::Int || right == ValueType::Int {
		return ValueType::Int;
	}
	if left == ValueType::Uint || right == ValueType::Uint {
		let is_signed_primitive = |t: &ValueType| {
			matches!(
				t,
				ValueType::Int1
					| ValueType::Int2 | ValueType::Int4 | ValueType::Int8
					| ValueType::Int16
			)
		};
		if is_signed_primitive(&left) || is_signed_primitive(&right) {
			return ValueType::Int;
		}
		return ValueType::Uint;
	}

	let rank = |t: &ValueType| match t {
		ValueType::Int1 | ValueType::Uint1 => 0,
		ValueType::Int2 | ValueType::Uint2 => 1,
		ValueType::Int4 | ValueType::Uint4 => 2,
		ValueType::Int8 | ValueType::Uint8 => 3,
		ValueType::Int16 | ValueType::Uint16 => 4,
		_ => 0,
	};
	let is_signed = |t: &ValueType| {
		matches!(t, ValueType::Int1 | ValueType::Int2 | ValueType::Int4 | ValueType::Int8 | ValueType::Int16)
	};
	let is_unsigned = |t: &ValueType| {
		matches!(
			t,
			ValueType::Uint1 | ValueType::Uint2 | ValueType::Uint4 | ValueType::Uint8 | ValueType::Uint16
		)
	};

	let max_rank = rank(&left).max(rank(&right));
	if is_signed(&left) && is_signed(&right) {
		return [ValueType::Int1, ValueType::Int2, ValueType::Int4, ValueType::Int8, ValueType::Int16]
			[max_rank]
			.clone();
	}
	if is_unsigned(&left) && is_unsigned(&right) {
		return [ValueType::Uint1, ValueType::Uint2, ValueType::Uint4, ValueType::Uint8, ValueType::Uint16]
			[max_rank]
			.clone();
	}
	let bumped = (max_rank + 1).min(4);
	[ValueType::Int1, ValueType::Int2, ValueType::Int4, ValueType::Int8, ValueType::Int16][bumped].clone()
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

pub fn convert_column_to_type(data: &ColumnBuffer, target: ValueType, row_count: usize) -> ColumnBuffer {
	match target {
		ValueType::Int1 => to_int1(data, row_count),
		ValueType::Int2 => to_int2(data, row_count),
		ValueType::Int4 => to_int4(data, row_count),
		ValueType::Int8 => to_int8(data, row_count),
		ValueType::Int16 => to_int16(data, row_count),
		ValueType::Uint1 => to_uint1(data, row_count),
		ValueType::Uint2 => to_uint2(data, row_count),
		ValueType::Uint4 => to_uint4(data, row_count),
		ValueType::Uint8 => to_uint8(data, row_count),
		ValueType::Uint16 => to_uint16(data, row_count),
		ValueType::Float4 => to_float4(data, row_count),
		ValueType::Float8 => to_float8(data, row_count),
		ValueType::Int => to_int(data, row_count),
		ValueType::Uint => to_uint(data, row_count),
		ValueType::Decimal => to_decimal(data, row_count),
		_ => data.clone(),
	}
}
