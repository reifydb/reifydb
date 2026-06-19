// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	error::TypeError,
	fragment::Fragment,
	value::{
		boolean::parse::parse_bool,
		decimal::parse::parse_decimal,
		int::Int,
		number::parse::{parse_float, parse_primitive_int, parse_primitive_uint},
		uint::Uint,
		value_type::ValueType,
	},
};
use temporal::TemporalParser;

use super::{temporal, uuid::UuidParser};
use crate::{Result, error::CastError};

pub(crate) struct TextParser;

impl TextParser {
	pub(crate) fn from_text(fragment: Fragment, target: ValueType, row_count: usize) -> Result<ColumnBuffer> {
		match target {
			ValueType::Boolean => Self::parse_bool(fragment, row_count),
			ValueType::Float4 => Self::parse_float4(fragment, row_count),
			ValueType::Float8 => Self::parse_float8(fragment, row_count),
			ValueType::Int1 => Self::parse_int1(fragment, row_count),
			ValueType::Int2 => Self::parse_int2(fragment, row_count),
			ValueType::Int4 => Self::parse_int4(fragment, row_count),
			ValueType::Int8 => Self::parse_int8(fragment, row_count),
			ValueType::Int16 => Self::parse_int16(fragment, row_count),
			ValueType::Uint1 => Self::parse_uint1(fragment, row_count),
			ValueType::Uint2 => Self::parse_uint2(fragment, row_count),
			ValueType::Uint4 => Self::parse_uint4(fragment, row_count),
			ValueType::Uint8 => Self::parse_uint8(fragment, row_count),
			ValueType::Uint16 => Self::parse_uint16(fragment, row_count),
			ValueType::Int => Self::parse_int(fragment, row_count),
			ValueType::Uint => Self::parse_uint(fragment, row_count),
			ValueType::Decimal => Self::parse_decimal(fragment, target, row_count),
			ValueType::Date => {
				TemporalParser::parse_temporal_type(fragment.clone(), ValueType::Date, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: ValueType::Date,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			ValueType::DateTime => {
				TemporalParser::parse_temporal_type(fragment.clone(), ValueType::DateTime, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: ValueType::DateTime,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			ValueType::Time => {
				TemporalParser::parse_temporal_type(fragment.clone(), ValueType::Time, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: ValueType::Time,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			ValueType::Duration => {
				TemporalParser::parse_temporal_type(fragment.clone(), ValueType::Duration, row_count)
					.map_err(|e| {
						CastError::InvalidTemporal {
							fragment,
							target: ValueType::Duration,
							cause: e.diagnostic(),
						}
						.into()
					})
			}
			ValueType::Uuid4 => UuidParser::from_text(fragment, ValueType::Uuid4, row_count),
			ValueType::Uuid7 => UuidParser::from_text(fragment, ValueType::Uuid7, row_count),
			ValueType::IdentityId => UuidParser::from_text(fragment, ValueType::IdentityId, row_count),
			_ => Err(TypeError::UnsupportedCast {
				from: ValueType::Utf8,
				to: target,
				fragment,
			}
			.into()),
		}
	}

	fn parse_bool(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		match parse_bool(fragment.clone()) {
			Ok(value) => Ok(ColumnBuffer::bool(vec![value; row_count])),
			Err(err) => Err(CastError::InvalidBoolean {
				fragment,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_float4(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		match parse_float::<f32>(fragment.clone()) {
			Ok(v) => Ok(ColumnBuffer::float4(vec![v; row_count])),
			Err(err) => Err(CastError::InvalidNumber {
				fragment,
				target: ValueType::Float4,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_float8(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		match parse_float::<f64>(fragment.clone()) {
			Ok(v) => Ok(ColumnBuffer::float8(vec![v; row_count])),
			Err(err) => Err(CastError::InvalidNumber {
				fragment,
				target: ValueType::Float8,
				cause: err.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_int1(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::int1(vec![
			match parse_primitive_int::<i8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Int1,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int2(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::int2(vec![
			match parse_primitive_int::<i16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Int2,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int4(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::int4(vec![
			match parse_primitive_int::<i32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Int4,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int8(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::int8(vec![
			match parse_primitive_int::<i64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Int8,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int16(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::int16(vec![
			match parse_primitive_int::<i128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Int16,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint1(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::uint1(vec![
			match parse_primitive_uint::<u8>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Uint1,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint2(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::uint2(vec![
			match parse_primitive_uint::<u16>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Uint2,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint4(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::uint4(vec![
			match parse_primitive_uint::<u32>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Uint4,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint8(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::uint8(vec![
			match parse_primitive_uint::<u64>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Uint8,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_uint16(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		Ok(ColumnBuffer::uint16(vec![
			match parse_primitive_uint::<u128>(fragment.clone()) {
				Ok(v) => v,
				Err(e) =>
					return Err(CastError::InvalidNumber {
						fragment,
						target: ValueType::Uint16,
						cause: e.diagnostic()
					}
					.into()),
			};
			row_count
		]))
	}

	fn parse_int(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		match parse_primitive_int::<Int>(fragment.clone()) {
			Ok(v) => Ok(ColumnBuffer::int(vec![v; row_count])),
			Err(e) => Err(CastError::InvalidNumber {
				fragment,
				target: ValueType::Int,
				cause: e.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_uint(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		match parse_primitive_uint::<Uint>(fragment.clone()) {
			Ok(v) => Ok(ColumnBuffer::uint(vec![v; row_count])),
			Err(e) => Err(CastError::InvalidNumber {
				fragment,
				target: ValueType::Uint,
				cause: e.diagnostic(),
			}
			.into()),
		}
	}

	fn parse_decimal(fragment: Fragment, target: ValueType, row_count: usize) -> Result<ColumnBuffer> {
		match parse_decimal(fragment.clone()) {
			Ok(v) => Ok(ColumnBuffer::decimal(vec![v; row_count])),
			Err(e) => Err(CastError::InvalidNumber {
				fragment,
				target,
				cause: e.diagnostic(),
			}
			.into()),
		}
	}
}
