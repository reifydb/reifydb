// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::columnar::ColumnData;
use reifydb_type::{
	IntoFragment, Type, diagnostic::cast, parse_bool, parse_float, parse_primitive_int, parse_primitive_uint,
	return_error,
};
use temporal::TemporalParser;

use super::{temporal, uuid::UuidParser};

pub(crate) struct TextParser;

impl TextParser {
	/// Parse text to a specific target type with detailed error handling
	pub(crate) fn from_text<'a>(
		fragment: impl IntoFragment<'a>,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match target {
			Type::Boolean => Self::parse_bool(fragment, row_count),
			Type::Float4 => Self::parse_float4(fragment, row_count),
			Type::Float8 => Self::parse_float8(fragment, row_count),
			Type::Int1 => Self::parse_int1(fragment, row_count),
			Type::Int2 => Self::parse_int2(fragment, row_count),
			Type::Int4 => Self::parse_int4(fragment, row_count),
			Type::Int8 => Self::parse_int8(fragment, row_count),
			Type::Int16 => Self::parse_int16(fragment, row_count),
			Type::Uint1 => Self::parse_uint1(fragment, row_count),
			Type::Uint2 => Self::parse_uint2(fragment, row_count),
			Type::Uint4 => Self::parse_uint4(fragment, row_count),
			Type::Uint8 => Self::parse_uint8(fragment, row_count),
			Type::Uint16 => Self::parse_uint16(fragment, row_count),
			Type::Date => TemporalParser::parse_temporal_type(fragment, Type::Date, row_count),
			Type::DateTime => TemporalParser::parse_temporal_type(fragment, Type::DateTime, row_count),
			Type::Time => TemporalParser::parse_temporal_type(fragment, Type::Time, row_count),
			Type::Interval => TemporalParser::parse_temporal_type(fragment, Type::Interval, row_count),
			Type::Uuid4 => UuidParser::from_text(fragment, Type::Uuid4, row_count),
			Type::Uuid7 => UuidParser::from_text(fragment, Type::Uuid7, row_count),
			_ => return_error!(cast::unsupported_cast(fragment, Type::Utf8, target)),
		}
	}

	fn parse_bool<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_bool(&fragment) {
			Ok(value) => Ok(ColumnData::bool(vec![value; row_count])),
			Err(err) => return_error!(cast::invalid_boolean(fragment, err.diagnostic())),
		}
	}

	fn parse_float4<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_float::<f32>(&fragment) {
			Ok(v) => Ok(ColumnData::float4(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(fragment, Type::Float4, err.diagnostic()))
			}
		}
	}

	fn parse_float8<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match parse_float::<f64>(&fragment) {
			Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(fragment, Type::Float8, err.diagnostic()))
			}
		}
	}

	fn parse_int1<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::int1(vec![
			match parse_primitive_int::<i8>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Int1, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_int2<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::int2(vec![
			match parse_primitive_int::<i16>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Int2, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_int4<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::int4(vec![
			match parse_primitive_int::<i32>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Int4, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_int8<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::int8(vec![
			match parse_primitive_int::<i64>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Int8, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_int16<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::int16(vec![
			match parse_primitive_int::<i128>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Int16, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_uint1<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::uint1(vec![
			match parse_primitive_uint::<u8>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Uint1, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_uint2<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::uint2(vec![
			match parse_primitive_uint::<u16>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Uint2, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_uint4<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::uint4(vec![
			match parse_primitive_uint::<u32>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Uint4, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_uint8<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::uint8(vec![
			match parse_primitive_uint::<u64>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Uint8, e.diagnostic())),
			};
			row_count
		]))
	}

	fn parse_uint16<'a>(fragment: impl IntoFragment<'a>, row_count: usize) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		Ok(ColumnData::uint16(vec![
			match parse_primitive_uint::<u128>(&fragment) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(fragment, Type::Uint16, e.diagnostic())),
			};
			row_count
		]))
	}
}
