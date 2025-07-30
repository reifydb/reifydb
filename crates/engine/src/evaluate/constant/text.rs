// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::ColumnData;
use reifydb_core::result::error::diagnostic::cast;
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::value::number::{parse_float, parse_int, parse_uint};
use reifydb_core::{return_error, Span, Type};
use temporal::TemporalParser;
use super::temporal;
use super::uuid::UuidParser;

pub(crate) struct TextParser;

impl TextParser {
    /// Parse text to a specific target type with detailed error handling
    pub(crate) fn from_text(
        span: impl Span,
        target: Type,
        row_count: usize,
    ) -> crate::Result<ColumnData> {
        match target {
            Type::Bool => Self::parse_bool(span, row_count),
            Type::Float4 => Self::parse_float4(span, row_count),
            Type::Float8 => Self::parse_float8(span, row_count),
            Type::Int1 => Self::parse_int1(span, row_count),
            Type::Int2 => Self::parse_int2(span, row_count),
            Type::Int4 => Self::parse_int4(span, row_count),
            Type::Int8 => Self::parse_int8(span, row_count),
            Type::Int16 => Self::parse_int16(span, row_count),
            Type::Uint1 => Self::parse_uint1(span, row_count),
            Type::Uint2 => Self::parse_uint2(span, row_count),
            Type::Uint4 => Self::parse_uint4(span, row_count),
            Type::Uint8 => Self::parse_uint8(span, row_count),
            Type::Uint16 => Self::parse_uint16(span, row_count),
            Type::Date => {
                TemporalParser::parse_temporal_type(span, Type::Date, row_count)
            }
            Type::DateTime => {
                TemporalParser::parse_temporal_type(span, Type::DateTime, row_count)
            }
            Type::Time => {
                TemporalParser::parse_temporal_type(span, Type::Time, row_count)
            }
            Type::Interval => {
                TemporalParser::parse_temporal_type(span, Type::Interval, row_count)
            }
            Type::Uuid4 => {
                UuidParser::from_text(span, Type::Uuid4, row_count)
            }
            Type::Uuid7 => {
                UuidParser::from_text(span, Type::Uuid7, row_count)
            }
            _ => return_error!(cast::unsupported_cast(span.to_owned(), Type::Utf8, target)),
        }
    }

    fn parse_bool(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        match parse_bool(span.clone()) {
            Ok(value) => Ok(ColumnData::bool(vec![value; row_count])),
            Err(err) => return_error!(cast::invalid_boolean(span.to_owned(), err.diagnostic())),
        }
    }

    fn parse_float4(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        match parse_float::<f32>(span.clone()) {
            Ok(v) => Ok(ColumnData::float4(vec![v; row_count])),
            Err(err) => {
                return_error!(cast::invalid_number(span.to_owned(), Type::Float4, err.diagnostic()))
            }
        }
    }

    fn parse_float8(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        match parse_float::<f64>(span.clone()) {
            Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
            Err(err) => {
                return_error!(cast::invalid_number(span.to_owned(), Type::Float8, err.diagnostic()))
            }
        }
    }

    fn parse_int1(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::int1(vec![
            match parse_int::<i8>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Int1, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_int2(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::int2(vec![
            match parse_int::<i16>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Int2, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_int4(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::int4(vec![
            match parse_int::<i32>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Int4, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_int8(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::int8(vec![
            match parse_int::<i64>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Int8, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_int16(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::int16(vec![
            match parse_int::<i128>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Int16, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_uint1(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::uint1(vec![
            match parse_uint::<u8>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Uint1, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_uint2(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::uint2(vec![
            match parse_uint::<u16>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Uint2, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_uint4(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::uint4(vec![
            match parse_uint::<u32>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Uint4, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_uint8(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::uint8(vec![
            match parse_uint::<u64>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Uint8, e.diagnostic())),
            };
            row_count
        ]))
    }

    fn parse_uint16(span: impl Span, row_count: usize) -> crate::Result<ColumnData> {
        Ok(ColumnData::uint16(vec![
            match parse_uint::<u128>(span.clone()) {
                Ok(v) => v,
                Err(e) => return_error!(cast::invalid_number(span.to_owned(), Type::Uint16, e.diagnostic())),
            };
            row_count
        ]))
    }
}
