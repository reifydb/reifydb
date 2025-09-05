// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::columnar::ColumnData;
use reifydb_type::{
	IntoFragment, Type,
	diagnostic::{cast, number},
	parse_bool, parse_float, parse_int, parse_uint, return_error,
};

pub(crate) struct NumberParser;

impl NumberParser {
	/// Parse a number to a specific target type with detailed error
	/// handling and range checking
	pub(crate) fn from_number<'a>(
		fragment: impl IntoFragment<'a>,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match target {
			Type::Boolean => Self::parse_bool(fragment, row_count),
			Type::Float4 => Self::parse_float4(fragment, row_count),
			Type::Float8 => Self::parse_float8(fragment, row_count),
			Type::Int1 => {
				Self::parse_int1(fragment, target, row_count)
			}
			Type::Int2 => {
				Self::parse_int2(fragment, target, row_count)
			}
			Type::Int4 => {
				Self::parse_int4(fragment, target, row_count)
			}
			Type::Int8 => {
				Self::parse_int8(fragment, target, row_count)
			}
			Type::Int16 => {
				Self::parse_int16(fragment, target, row_count)
			}
			Type::Uint1 => {
				Self::parse_uint1(fragment, target, row_count)
			}
			Type::Uint2 => {
				Self::parse_uint2(fragment, target, row_count)
			}
			Type::Uint4 => {
				Self::parse_uint4(fragment, target, row_count)
			}
			Type::Uint8 => {
				Self::parse_uint8(fragment, target, row_count)
			}
			Type::Uint16 => {
				Self::parse_uint16(fragment, target, row_count)
			}
			Type::VarInt => Self::parse_varint(fragment, row_count),
			Type::VarUint => {
				Self::parse_varuint(fragment, row_count)
			}
			Type::Decimal {
				..
			} => Self::parse_decimal(fragment, row_count),
			_ => return_error!(cast::unsupported_cast(
				fragment,
				Type::Float8,
				target,
			)),
		}
	}

	fn parse_bool<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_bool(&fragment) {
			Ok(v) => Ok(ColumnData::bool(vec![v; row_count])),
			Err(err) => return_error!(cast::invalid_boolean(
				fragment,
				err.diagnostic()
			)),
		}
	}

	fn parse_float4<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_float::<f32>(&fragment) {
			Ok(v) => Ok(ColumnData::float4(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(
					fragment,
					Type::Float4,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_float8<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_float::<f64>(&fragment) {
			Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(
					fragment,
					Type::Float8,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_int1<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_int::<i8>(&fragment) {
			Ok(ColumnData::int1(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= i8::MIN as f64
				&& truncated <= i8::MAX as f64
			{
				Ok(ColumnData::int1(vec![
					truncated as i8;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			match parse_int::<i8>(&fragment) {
				Ok(_) => unreachable!(),
				Err(err) => {
					return_error!(cast::invalid_number(
						fragment,
						ty,
						err.diagnostic()
					))
				}
			}
		}
	}

	fn parse_int2<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_int::<i16>(&fragment) {
			Ok(ColumnData::int2(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= i16::MIN as f64
				&& truncated <= i16::MAX as f64
			{
				Ok(ColumnData::int2(vec![
					truncated as i16;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_int4<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_int::<i32>(&fragment) {
			Ok(ColumnData::int4(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= i32::MIN as f64
				&& truncated <= i32::MAX as f64
			{
				Ok(ColumnData::int4(vec![
					truncated as i32;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_int8<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_int::<i64>(&fragment) {
			Ok(ColumnData::int8(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= i64::MIN as f64
				&& truncated <= i64::MAX as f64
			{
				Ok(ColumnData::int8(vec![
					truncated as i64;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_int16<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_int::<i128>(&fragment) {
			Ok(ColumnData::int16(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= i128::MIN as f64
				&& truncated <= i128::MAX as f64
			{
				Ok(ColumnData::int16(vec![
					truncated as i128;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_uint1<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_uint::<u8>(&fragment) {
			Ok(ColumnData::uint1(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u8::MAX as f64 {
				Ok(ColumnData::uint1(vec![
					truncated as u8;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_uint2<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_uint::<u16>(&fragment) {
			Ok(ColumnData::uint2(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u16::MAX as f64 {
				Ok(ColumnData::uint2(vec![
					truncated as u16;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_uint4<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_uint::<u32>(&fragment) {
			Ok(ColumnData::uint4(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u32::MAX as f64 {
				Ok(ColumnData::uint4(vec![
					truncated as u32;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_uint8<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_uint::<u64>(&fragment) {
			Ok(ColumnData::uint8(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u64::MAX as f64 {
				Ok(ColumnData::uint8(vec![
					truncated as u64;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_uint16<'a>(
		fragment: impl IntoFragment<'a>,
		ty: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		if let Ok(v) = parse_uint::<u128>(&fragment) {
			Ok(ColumnData::uint16(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(&fragment) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u128::MAX as f64 {
				Ok(ColumnData::uint16(vec![
					truncated as u128;
					row_count
				]))
			} else {
				return_error!(cast::invalid_number(
					&fragment,
					ty,
					number::number_out_of_range(
						&fragment, ty, None
					),
				))
			}
		} else {
			return_error!(cast::invalid_number(
				&fragment,
				ty,
				number::invalid_number_format(&fragment, ty),
			))
		}
	}

	fn parse_varint<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match reifydb_type::parse_varint(&fragment) {
			Ok(v) => Ok(ColumnData::varint(vec![v; row_count])),
			Err(err) => return_error!(cast::invalid_number(
				fragment,
				Type::VarInt,
				err.diagnostic()
			)),
		}
	}

	fn parse_varuint<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match reifydb_type::parse_varuint(&fragment) {
			Ok(v) => Ok(ColumnData::varuint(vec![v; row_count])),
			Err(err) => return_error!(cast::invalid_number(
				fragment,
				Type::VarUint,
				err.diagnostic()
			)),
		}
	}

	fn parse_decimal<'a>(
		fragment: impl IntoFragment<'a>,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match reifydb_type::parse_decimal(&fragment) {
			Ok(v) => Ok(ColumnData::decimal(vec![v; row_count])),
			Err(err) => return_error!(cast::invalid_number(
				fragment,
				Type::Decimal {
					precision: reifydb_type::value::decimal::Precision::new(38),
					scale: reifydb_type::value::decimal::Scale::new(0),
				},
				err.diagnostic()
			)),
		}
	}
}
