// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::frame::ColumnValues;
use reifydb_core::error::diagnostic::{cast, number};
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::value::number::{parse_float, parse_int, parse_uint};
use reifydb_core::{return_error, Span, Type};

pub(crate) struct NumberParser;

impl NumberParser {
    /// Parse a number to a specific target type with detailed error handling and range checking
    pub(crate) fn from_number(
        span: impl Span,
        target: Type,
        row_count: usize,
    ) -> crate::Result<ColumnValues> {
        match target {
            Type::Bool => Self::parse_bool(span, row_count),
            Type::Float4 => Self::parse_float4(span, row_count),
            Type::Float8 => Self::parse_float8(span, row_count),
            Type::Int1 => Self::parse_int1(span, target, row_count),
            Type::Int2 => Self::parse_int2(span, target, row_count),
            Type::Int4 => Self::parse_int4(span, target, row_count),
            Type::Int8 => Self::parse_int8(span, target, row_count),
            Type::Int16 => Self::parse_int16(span, target, row_count),
            Type::Uint1 => Self::parse_uint1(span, target, row_count),
            Type::Uint2 => Self::parse_uint2(span, target, row_count),
            Type::Uint4 => Self::parse_uint4(span, target, row_count),
            Type::Uint8 => Self::parse_uint8(span, target, row_count),
            Type::Uint16 => Self::parse_uint16(span, target, row_count),
            _ => return_error!(cast::unsupported_cast(
                span.to_owned(),
                Type::Float8, // Numbers are treated as float8 by default
                target,
            )),
        }
    }

    fn parse_bool(span: impl Span, row_count: usize) -> crate::Result<ColumnValues> {
        match parse_bool(span.clone()) {
            Ok(v) => Ok(ColumnValues::bool(vec![v; row_count])),
            Err(err) => return_error!(cast::invalid_boolean(span.to_owned(), err.diagnostic())),
        }
    }

    fn parse_float4(span: impl Span, row_count: usize) -> crate::Result<ColumnValues> {
        match parse_float::<f32>(span.clone()) {
            Ok(v) => Ok(ColumnValues::float4(vec![v; row_count])),
            Err(err) => {
                return_error!(cast::invalid_number(span.to_owned(), Type::Float4, err.diagnostic()))
            }
        }
    }

    fn parse_float8(span: impl Span, row_count: usize) -> crate::Result<ColumnValues> {
        match parse_float::<f64>(span.clone()) {
            Ok(v) => Ok(ColumnValues::float8(vec![v; row_count])),
            Err(err) => {
                return_error!(cast::invalid_number(span.to_owned(), Type::Float8, err.diagnostic()))
            }
        }
    }

    fn parse_int1(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_int::<i8>(span.clone()) {
            Ok(ColumnValues::int1(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                Ok(ColumnValues::int1(vec![truncated as i8; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            match parse_int::<i8>(span.clone()) {
                Ok(_) => unreachable!(),
                Err(err) => return_error!(cast::invalid_number(span.to_owned(), ty, err.diagnostic())),
            }
        }
    }

    fn parse_int2(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_int::<i16>(span.clone()) {
            Ok(ColumnValues::int2(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                Ok(ColumnValues::int2(vec![truncated as i16; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_int4(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_int::<i32>(span.clone()) {
            Ok(ColumnValues::int4(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                Ok(ColumnValues::int4(vec![truncated as i32; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_int8(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_int::<i64>(span.clone()) {
            Ok(ColumnValues::int8(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                Ok(ColumnValues::int8(vec![truncated as i64; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_int16(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_int::<i128>(span.clone()) {
            Ok(ColumnValues::int16(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                Ok(ColumnValues::int16(vec![truncated as i128; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_uint1(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_uint::<u8>(span.clone()) {
            Ok(ColumnValues::uint1(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                Ok(ColumnValues::uint1(vec![truncated as u8; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_uint2(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_uint::<u16>(span.clone()) {
            Ok(ColumnValues::uint2(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                Ok(ColumnValues::uint2(vec![truncated as u16; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_uint4(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_uint::<u32>(span.clone()) {
            Ok(ColumnValues::uint4(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                Ok(ColumnValues::uint4(vec![truncated as u32; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_uint8(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_uint::<u64>(span.clone()) {
            Ok(ColumnValues::uint8(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                Ok(ColumnValues::uint8(vec![truncated as u64; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }

    fn parse_uint16(span: impl Span, ty: Type, row_count: usize) -> crate::Result<ColumnValues> {
        if let Ok(v) = parse_uint::<u128>(span.clone()) {
            Ok(ColumnValues::uint16(vec![v; row_count]))
        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
            let truncated = f.trunc();
            if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                Ok(ColumnValues::uint16(vec![truncated as u128; row_count]))
            } else {
                return_error!(cast::invalid_number(
                    span.clone().to_owned(),
                    ty,
                    number::number_out_of_range(span.clone().to_owned(), ty),
                ))
            }
        } else {
            return_error!(cast::invalid_number(
                span.clone().to_owned(),
                ty,
                number::invalid_number_format(span.to_owned(), ty),
            ))
        }
    }
}