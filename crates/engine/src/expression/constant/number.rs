// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::{IntoDiagnostic, TypeError},
	fragment::Fragment,
	value::{
		boolean::parse::parse_bool,
		decimal::parse::parse_decimal,
		number::parse::{parse_float, parse_primitive_int, parse_primitive_uint},
		r#type::Type,
	},
};

use crate::error::CastError;

pub(crate) struct NumberParser;

impl NumberParser {
	/// Parse a number to a specific target type with detailed error
	/// handling and range checking
	pub(crate) fn from_number<'a>(fragment: Fragment, target: Type, row_count: usize) -> crate::Result<ColumnData> {
		match &target {
			Type::Boolean => Self::parse_bool(fragment, row_count),
			Type::Float4 => Self::parse_float4(fragment, row_count),
			Type::Float8 => Self::parse_float8(fragment, row_count),
			Type::Int1 => Self::parse_int1(fragment, target, row_count),
			Type::Int2 => Self::parse_int2(fragment, target, row_count),
			Type::Int4 => Self::parse_int4(fragment, target, row_count),
			Type::Int8 => Self::parse_int8(fragment, target, row_count),
			Type::Int16 => Self::parse_int16(fragment, target, row_count),
			Type::Uint1 => Self::parse_uint1(fragment, target, row_count),
			Type::Uint2 => Self::parse_uint2(fragment, target, row_count),
			Type::Uint4 => Self::parse_uint4(fragment, target, row_count),
			Type::Uint8 => Self::parse_uint8(fragment, target, row_count),
			Type::Uint16 => Self::parse_uint16(fragment, target, row_count),
			Type::Int => Self::parse_int(fragment, row_count),
			Type::Uint => Self::parse_uint(fragment, row_count),
			Type::Decimal {
				..
			} => Self::parse_decimal(fragment, row_count),
			_ => {
				return Err(TypeError::UnsupportedCast {
					from: Type::Float8,
					to: target,
					fragment,
				}
				.into());
			}
		}
	}

	fn parse_bool<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
		match parse_bool(fragment.clone()) {
			Ok(v) => Ok(ColumnData::bool(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidBoolean {
					fragment,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_float4<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
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

	fn parse_float8<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
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

	fn parse_int1<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_int::<i8>(fragment.clone()) {
			Ok(ColumnData::int1(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
				Ok(ColumnData::int1(vec![truncated as i8; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			match parse_primitive_int::<i8>(fragment.clone()) {
				Ok(_) => unreachable!(),
				Err(err) => {
					return Err(CastError::InvalidNumber {
						fragment,
						target: ty,
						cause: err.diagnostic(),
					}
					.into());
				}
			}
		}
	}

	fn parse_int2<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_int::<i16>(fragment.clone()) {
			Ok(ColumnData::int2(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
				Ok(ColumnData::int2(vec![truncated as i16; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_int4<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_int::<i32>(fragment.clone()) {
			Ok(ColumnData::int4(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
				Ok(ColumnData::int4(vec![truncated as i32; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_int8<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_int::<i64>(fragment.clone()) {
			Ok(ColumnData::int8(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
				Ok(ColumnData::int8(vec![truncated as i64; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_int16<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_int::<i128>(fragment.clone()) {
			Ok(ColumnData::int16(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
				Ok(ColumnData::int16(vec![truncated as i128; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_uint1<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_uint::<u8>(fragment.clone()) {
			Ok(ColumnData::uint1(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u8::MAX as f64 {
				Ok(ColumnData::uint1(vec![truncated as u8; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_uint2<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_uint::<u16>(fragment.clone()) {
			Ok(ColumnData::uint2(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u16::MAX as f64 {
				Ok(ColumnData::uint2(vec![truncated as u16; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_uint4<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_uint::<u32>(fragment.clone()) {
			Ok(ColumnData::uint4(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u32::MAX as f64 {
				Ok(ColumnData::uint4(vec![truncated as u32; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_uint8<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_uint::<u64>(fragment.clone()) {
			Ok(ColumnData::uint8(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u64::MAX as f64 {
				Ok(ColumnData::uint8(vec![truncated as u64; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_uint16<'a>(fragment: Fragment, ty: Type, row_count: usize) -> crate::Result<ColumnData> {
		if let Ok(v) = parse_primitive_uint::<u128>(fragment.clone()) {
			Ok(ColumnData::uint16(vec![v; row_count]))
		} else if let Ok(f) = parse_float::<f64>(fragment.clone()) {
			let truncated = f.trunc();
			if truncated >= 0.0 && truncated <= u128::MAX as f64 {
				Ok(ColumnData::uint16(vec![truncated as u128; row_count]))
			} else {
				return Err(CastError::InvalidNumber {
					fragment: fragment.clone(),
					target: ty.clone(),
					cause: TypeError::NumberOutOfRange {
						target: ty,
						fragment,
						descriptor: None,
					}
					.into_diagnostic(),
				}
				.into());
			}
		} else {
			return Err(CastError::InvalidNumber {
				fragment: fragment.clone(),
				target: ty.clone(),
				cause: TypeError::InvalidNumberFormat {
					target: ty,
					fragment,
				}
				.into_diagnostic(),
			}
			.into());
		}
	}

	fn parse_int<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
		match parse_primitive_int(fragment.clone()) {
			Ok(v) => Ok(ColumnData::int(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Int,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_uint<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
		match parse_primitive_uint(fragment.clone()) {
			Ok(v) => Ok(ColumnData::uint(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Uint,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}

	fn parse_decimal<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
		match parse_decimal(fragment.clone()) {
			Ok(v) => Ok(ColumnData::decimal(vec![v; row_count])),
			Err(err) => {
				return Err(CastError::InvalidNumber {
					fragment,
					target: Type::Decimal,
					cause: err.diagnostic(),
				}
				.into());
			}
		}
	}
}
