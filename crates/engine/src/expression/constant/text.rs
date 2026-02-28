// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::TypeError,
	fragment::Fragment,
	value::{
		boolean::parse::parse_bool,
		decimal::parse::parse_decimal,
		int::Int,
		number::parse::{parse_float, parse_primitive_int, parse_primitive_uint},
		r#type::Type,
		uint::Uint,
	},
};
use temporal::TemporalParser;

use super::{temporal, uuid::UuidParser};
use crate::{Result, error::CastError};

pub(crate) struct TextParser;

impl TextParser {
	/// Parse text to a specific target type with detailed error handling
	pub(crate) fn from_text<'a>(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnData> {
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
			Type::Int => Self::parse_int(fragment, row_count),
			Type::Uint => Self::parse_uint(fragment, row_count),
			Type::Decimal {
				..
			} => Self::parse_decimal(fragment, target, row_count),
			Type::Date => TemporalParser::parse_temporal_type(fragment.clone(), Type::Date, row_count)
				.map_err(|e| {
					CastError::InvalidTemporal {
						fragment,
						target: Type::Date,
						cause: e.diagnostic(),
					}
					.into()
				}),
			Type::DateTime => {
				TemporalParser::parse_temporal_type(fragment.clone(), Type::DateTime, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: Type::DateTime,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			Type::Time => TemporalParser::parse_temporal_type(fragment.clone(), Type::Time, row_count)
				.map_err(|e| {
					CastError::InvalidTemporal {
						fragment,
						target: Type::Time,
						cause: e.diagnostic(),
					}
					.into()
				}),
			Type::Duration => {
				TemporalParser::parse_temporal_type(fragment.clone(), Type::Duration, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: Type::Duration,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			Type::Uuid4 => UuidParser::from_text(fragment, Type::Uuid4, row_count),
			Type::Uuid7 => UuidParser::from_text(fragment, Type::Uuid7, row_count),
			_ => {
				return Err(TypeError::UnsupportedCast {
					from: Type::Utf8,
					to: target,
					fragment,
				}
				.into());
			}
		}
	}

	fn parse_bool<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		match parse_bool(fragment.clone()) {
			Ok(value) => Ok(ColumnData::bool(vec![value; row_count])),
			Err(err) => {
				return Err(CastError::InvalidBoolean {
					fragment,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_float4<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		match parse_float::<f32>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::float4(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Float4,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_float8<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		match parse_float::<f64>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Float8,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_int1<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::int1(vec![
			match parse_primitive_int::<i8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Int1,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int2<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::int2(vec![
			match parse_primitive_int::<i16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Int2,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int4<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::int4(vec![
			match parse_primitive_int::<i32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Int4,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int8<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::int8(vec![
			match parse_primitive_int::<i64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Int8,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int16<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::int16(vec![
			match parse_primitive_int::<i128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Int16,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint1<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::uint1(vec![
			match parse_primitive_uint::<u8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Uint1,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint2<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::uint2(vec![
			match parse_primitive_uint::<u16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Uint2,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint4<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::uint4(vec![
			match parse_primitive_uint::<u32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Uint4,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint8<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::uint8(vec![
			match parse_primitive_uint::<u64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Uint8,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint16<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		Ok(ColumnData::uint16(vec![
			match parse_primitive_uint::<u128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: Type::Uint16,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		match parse_primitive_int::<Int>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::int(vec![v; row_count])),
			Err(e) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Int,
					cause: e.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_uint<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		match parse_primitive_uint::<Uint>(fragment.clone()) {
			Ok(v) => Ok(ColumnData::uint(vec![v; row_count])),
			Err(e) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Uint,
					cause: e.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_decimal<'a>(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnData> {
		match parse_decimal(fragment.clone()) {
			Ok(v) => Ok(ColumnData::decimal(vec![v; row_count])),
			Err(e) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target,
					cause: e.diagnostic(),
				}
				.into());
			}
		}
	}
}
