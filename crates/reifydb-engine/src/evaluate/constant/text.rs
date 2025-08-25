// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	IntoFragment, OwnedFragment, Type,
	result::error::diagnostic::cast,
	return_error,
	value::{
		boolean::parse_bool,
		number::{parse_float, parse_int, parse_uint},
	},
};
use temporal::TemporalParser;

use super::{temporal, uuid::UuidParser};
use crate::columnar::ColumnData;

pub(crate) struct TextParser;

impl TextParser {
	/// Parse text to a specific target type with detailed error handling
	pub(crate) fn from_text(
		fragment: impl IntoFragment,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let fragment = fragment.into_fragment();
		match target {
			Type::Bool => Self::parse_bool(&fragment, row_count),
			Type::Float4 => {
				Self::parse_float4(&fragment, row_count)
			}
			Type::Float8 => {
				Self::parse_float8(&fragment, row_count)
			}
			Type::Int1 => Self::parse_int1(&fragment, row_count),
			Type::Int2 => Self::parse_int2(&fragment, row_count),
			Type::Int4 => Self::parse_int4(&fragment, row_count),
			Type::Int8 => Self::parse_int8(&fragment, row_count),
			Type::Int16 => Self::parse_int16(&fragment, row_count),
			Type::Uint1 => Self::parse_uint1(&fragment, row_count),
			Type::Uint2 => Self::parse_uint2(&fragment, row_count),
			Type::Uint4 => Self::parse_uint4(&fragment, row_count),
			Type::Uint8 => Self::parse_uint8(&fragment, row_count),
			Type::Uint16 => {
				Self::parse_uint16(&fragment, row_count)
			}
			Type::Date => TemporalParser::parse_temporal_type(
				fragment,
				Type::Date,
				row_count,
			),
			Type::DateTime => TemporalParser::parse_temporal_type(
				fragment,
				Type::DateTime,
				row_count,
			),
			Type::Time => TemporalParser::parse_temporal_type(
				fragment,
				Type::Time,
				row_count,
			),
			Type::Interval => TemporalParser::parse_temporal_type(
				fragment,
				Type::Interval,
				row_count,
			),
			Type::Uuid4 => UuidParser::from_text(
				fragment,
				Type::Uuid4,
				row_count,
			),
			Type::Uuid7 => UuidParser::from_text(
				fragment,
				Type::Uuid7,
				row_count,
			),
			_ => return_error!(cast::unsupported_cast(
				fragment.clone(),
				Type::Utf8,
				target
			)),
		}
	}

	fn parse_bool(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		match parse_bool(fragment.clone()) {
			Ok(value) => {
				Ok(ColumnData::bool(vec![value; row_count]))
			}
			Err(err) => return_error!(cast::invalid_boolean(
				fragment.clone(),
				err.diagnostic()
			)),
		}
	}

	fn parse_float4(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		match parse_float::<f32>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::float4(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Float4,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_float8(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		match parse_float::<f64>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
			Err(err) => {
				return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Float8,
					err.diagnostic()
				))
			}
		}
	}

	fn parse_int1(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::int1(vec![
			match parse_int::<i8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Int1,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_int2(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::int2(vec![
			match parse_int::<i16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Int2,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_int4(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::int4(vec![
			match parse_int::<i32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Int4,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_int8(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::int8(vec![
			match parse_int::<i64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Int8,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_int16(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::int16(vec![
			match parse_int::<i128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Int16,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_uint1(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::uint1(vec![
			match parse_uint::<u8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Uint1,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_uint2(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::uint2(vec![
			match parse_uint::<u16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Uint2,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_uint4(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::uint4(vec![
			match parse_uint::<u32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Uint4,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_uint8(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::uint8(vec![
			match parse_uint::<u64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Uint8,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}

	fn parse_uint16(
		fragment: &OwnedFragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(ColumnData::uint16(vec![
			match parse_uint::<u128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) => return_error!(cast::invalid_number(
					fragment.clone(),
					Type::Uint16,
					e.diagnostic()
				)),
			};
			row_count
		]))
	}
}
