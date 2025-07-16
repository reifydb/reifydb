// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod date;
mod datetime;
mod interval;
mod time;

use crate::evaluate;
use crate::evaluate::constant::date::parse_date;
use crate::evaluate::constant::datetime::parse_datetime;
use crate::evaluate::constant::interval::parse_interval;
use crate::evaluate::constant::time::parse_time;
use crate::evaluate::{Error, EvaluationContext, Evaluator};
use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::num::parse_float;
use reifydb_core::{DataType, Span};
use reifydb_core::diagnostic::cast;
use reifydb_core::diagnostic::number;
use reifydb_core::diagnostic::temporal;
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: &ConstantExpression,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value(&expr, row_count)?,
        })
    }

    pub(crate) fn constant_of(
        &mut self,
        expr: &ConstantExpression,
        data_type: DataType,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value_of(&expr, data_type, row_count)?,
        })
    }

    fn constant_value(
        expr: &ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }
            ConstantExpression::Number { span } => {
                let s = &span.fragment.replace("_", "");

                if s.contains(".") || s.contains("e") {
                    if let Ok(v) = parse_float(s) {
                        return Ok(ColumnValues::float8(vec![v; row_count]));
                    }
                    return Err(Error(number::invalid_number_format(span.clone(), DataType::Float8)));
                }

                if let Ok(v) = s.parse::<i8>() {
                    ColumnValues::int1(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i16>() {
                    ColumnValues::int2(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i32>() {
                    ColumnValues::int4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i64>() {
                    ColumnValues::int8(vec![v; row_count])
                } else if let Ok(v) = s.parse::<i128>() {
                    ColumnValues::int16(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u128>() {
                    ColumnValues::uint16(vec![v; row_count])
                } else {
                    return Err(Error(number::invalid_number_format(span.clone(), DataType::Uint16)));
                }
            }
            ConstantExpression::Text { span } => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Temporal { span } => Self::parse_temporal(span, row_count)?,
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }

    fn constant_value_of(
        expr: &ConstantExpression,
        data_type: DataType,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match (expr, data_type) {
            (ConstantExpression::Bool { span }, DataType::Bool) => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }

            // Bool to numeric types
            (ConstantExpression::Bool { span }, DataType::Float4) => {
                let value = if span.fragment == "true" { 1.0f32 } else { 0.0f32 };
                ColumnValues::float4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Float8) => {
                let value = if span.fragment == "true" { 1.0f64 } else { 0.0f64 };
                ColumnValues::float8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Int1) => {
                let value = if span.fragment == "true" { 1i8 } else { 0i8 };
                ColumnValues::int1(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Int2) => {
                let value = if span.fragment == "true" { 1i16 } else { 0i16 };
                ColumnValues::int2(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Int4) => {
                let value = if span.fragment == "true" { 1i32 } else { 0i32 };
                ColumnValues::int4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Int8) => {
                let value = if span.fragment == "true" { 1i64 } else { 0i64 };
                ColumnValues::int8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Int16) => {
                let value = if span.fragment == "true" { 1i128 } else { 0i128 };
                ColumnValues::int16(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Uint1) => {
                let value = if span.fragment == "true" { 1u8 } else { 0u8 };
                ColumnValues::uint1(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Uint2) => {
                let value = if span.fragment == "true" { 1u16 } else { 0u16 };
                ColumnValues::uint2(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Uint4) => {
                let value = if span.fragment == "true" { 1u32 } else { 0u32 };
                ColumnValues::uint4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Uint8) => {
                let value = if span.fragment == "true" { 1u64 } else { 0u64 };
                ColumnValues::uint8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, DataType::Uint16) => {
                let value = if span.fragment == "true" { 1u128 } else { 0u128 };
                ColumnValues::uint16(vec![value; row_count])
            }

            (ConstantExpression::Number { span }, ty) => {
                let s = &span.fragment.replace("_", "");
                match ty {
                    DataType::Bool => {
                        // Convert number to boolean (0 -> false, non-zero -> true)
                        if let Ok(f) = s.parse::<f64>() {
                            let value = f != 0.0;
                            ColumnValues::bool(vec![value; row_count])
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }

                    DataType::Float4 => match s.parse::<f32>() {
                        Ok(v) => ColumnValues::float4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    },
                    DataType::Float8 => match s.parse::<f64>() {
                        Ok(v) => ColumnValues::float8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    },
                    DataType::Int1 => {
                        if let Ok(v) = s.parse::<i8>() {
                            ColumnValues::int1(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                                ColumnValues::int1(vec![truncated as i8; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Int2 => {
                        if let Ok(v) = s.parse::<i16>() {
                            ColumnValues::int2(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                                ColumnValues::int2(vec![truncated as i16; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Int4 => {
                        if let Ok(v) = s.parse::<i32>() {
                            ColumnValues::int4(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                                ColumnValues::int4(vec![truncated as i32; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Int8 => {
                        if let Ok(v) = s.parse::<i64>() {
                            ColumnValues::int8(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                                ColumnValues::int8(vec![truncated as i64; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Int16 => {
                        if let Ok(v) = s.parse::<i128>() {
                            ColumnValues::int16(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                                ColumnValues::int16(vec![truncated as i128; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Uint1 => {
                        if let Ok(v) = s.parse::<u8>() {
                            ColumnValues::uint1(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                                ColumnValues::uint1(vec![truncated as u8; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Uint2 => {
                        if let Ok(v) = s.parse::<u16>() {
                            ColumnValues::uint2(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                                ColumnValues::uint2(vec![truncated as u16; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Uint4 => {
                        if let Ok(v) = s.parse::<u32>() {
                            ColumnValues::uint4(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                                ColumnValues::uint4(vec![truncated as u32; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Uint8 => {
                        if let Ok(v) = s.parse::<u64>() {
                            ColumnValues::uint8(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                                ColumnValues::uint8(vec![truncated as u64; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    DataType::Uint16 => {
                        if let Ok(v) = s.parse::<u128>() {
                            ColumnValues::uint16(vec![v; row_count])
                        } else if let Ok(f) = s.parse::<f64>() {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                                ColumnValues::uint16(vec![truncated as u128; row_count])
                            } else {
                                return Err(Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }

                    _ => {
                        return Err(Error(cast::unsupported_cast(
                            span.clone(),
                            DataType::Float8, // Numbers are treated as float8 by default
                            ty,
                        )));
                    }
                }
            }

            (ConstantExpression::Text { span }, DataType::Utf8) => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }

            // Text to numeric types
            (ConstantExpression::Text { span }, DataType::Bool) => {
                let s = &span.fragment;
                if s == "true" {
                    ColumnValues::bool(vec![true; row_count])
                } else if s == "false" {
                    ColumnValues::bool(vec![false; row_count])
                } else {
                    ColumnValues::undefined(row_count)
                }
            }
            (ConstantExpression::Text { span }, DataType::Float4) => {
                let s = &span.fragment.replace("_", "");
                match s.parse::<f32>() {
                    Ok(v) => ColumnValues::float4(vec![v; row_count]),
                    Err(_) => {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Float4,
                            number::invalid_number_format(span.clone(), DataType::Float4),
                        )));
                    }
                }
            }
            (ConstantExpression::Text { span }, DataType::Float8) => {
                let s = &span.fragment.replace("_", "");
                match s.parse::<f64>() {
                    Ok(v) => ColumnValues::float8(vec![v; row_count]),
                    Err(_) => {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Float8,
                            number::invalid_number_format(span.clone(), DataType::Float8),
                        )));
                    }
                }
            }
            (ConstantExpression::Text { span }, DataType::Int1) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<i8>() {
                    ColumnValues::int1(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                        ColumnValues::int1(vec![truncated as i8; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Int1,
                            number::number_out_of_range(span.clone(), DataType::Int1),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Int1,
                        number::invalid_number_format(span.clone(), DataType::Int1),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Int2) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<i16>() {
                    ColumnValues::int2(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                        ColumnValues::int2(vec![truncated as i16; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Int2,
                            number::number_out_of_range(span.clone(), DataType::Int2),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Int2,
                        number::invalid_number_format(span.clone(), DataType::Int2),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Int4) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<i32>() {
                    ColumnValues::int4(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                        ColumnValues::int4(vec![truncated as i32; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Int4,
                            number::number_out_of_range(span.clone(), DataType::Int4),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Int4,
                        number::invalid_number_format(span.clone(), DataType::Int4),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Int8) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<i64>() {
                    ColumnValues::int8(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                        ColumnValues::int8(vec![truncated as i64; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Int8,
                            number::number_out_of_range(span.clone(), DataType::Int8),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Int8,
                        number::invalid_number_format(span.clone(), DataType::Int8),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Int16) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<i128>() {
                    ColumnValues::int16(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                        ColumnValues::int16(vec![truncated as i128; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Int16,
                            number::number_out_of_range(span.clone(), DataType::Int16),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Int16,
                        number::invalid_number_format(span.clone(), DataType::Int16),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Uint1) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<u8>() {
                    ColumnValues::uint1(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                        ColumnValues::uint1(vec![truncated as u8; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Uint1,
                            number::number_out_of_range(span.clone(), DataType::Uint1),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Uint1,
                        number::invalid_number_format(span.clone(), DataType::Uint1),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Uint2) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<u16>() {
                    ColumnValues::uint2(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                        ColumnValues::uint2(vec![truncated as u16; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Uint2,
                            number::number_out_of_range(span.clone(), DataType::Uint2),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Uint2,
                        number::invalid_number_format(span.clone(), DataType::Uint2),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Uint4) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<u32>() {
                    ColumnValues::uint4(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                        ColumnValues::uint4(vec![truncated as u32; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Uint4,
                            number::number_out_of_range(span.clone(), DataType::Uint4),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Uint4,
                        number::invalid_number_format(span.clone(), DataType::Uint4),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Uint8) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<u64>() {
                    ColumnValues::uint8(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                        ColumnValues::uint8(vec![truncated as u64; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Uint8,
                            number::number_out_of_range(span.clone(), DataType::Uint8),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Uint8,
                        number::invalid_number_format(span.clone(), DataType::Uint8),
                    )));
                }
            }
            (ConstantExpression::Text { span }, DataType::Uint16) => {
                let s = &span.fragment.replace("_", "");
                if let Ok(v) = s.parse::<u128>() {
                    ColumnValues::uint16(vec![v; row_count])
                } else if let Ok(f) = s.parse::<f64>() {
                    let truncated = f.trunc();
                    if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                        ColumnValues::uint16(vec![truncated as u128; row_count])
                    } else {
                        return Err(Error(cast::invalid_number(
                            span.clone(),
                            DataType::Uint16,
                            number::number_out_of_range(span.clone(), DataType::Uint16),
                        )));
                    }
                } else {
                    return Err(Error(cast::invalid_number(
                        span.clone(),
                        DataType::Uint16,
                        number::invalid_number_format(span.clone(), DataType::Uint16),
                    )));
                }
            }

            (ConstantExpression::Text { span }, DataType::Date) => {
                let date = parse_date(span).map_err(|e| {
                    Error(cast::invalid_temporal(span.clone(), DataType::Date, e.diagnostic()))
                })?;
                ColumnValues::date(vec![date; row_count])
            }
            (ConstantExpression::Text { span }, DataType::DateTime) => {
                let datetime = parse_datetime(span).map_err(|e| {
                    Error(cast::invalid_temporal(span.clone(), DataType::DateTime, e.diagnostic()))
                })?;
                ColumnValues::datetime(vec![datetime; row_count])
            }
            (ConstantExpression::Text { span }, DataType::Time) => {
                let time = parse_time(span).map_err(|e| {
                    Error(cast::invalid_temporal(span.clone(), DataType::Time, e.diagnostic()))
                })?;
                ColumnValues::time(vec![time; row_count])
            }
            (ConstantExpression::Text { span }, DataType::Interval) => {
                let interval = parse_interval(span).map_err(|e| {
                    Error(cast::invalid_temporal(span.clone(), DataType::Interval, e.diagnostic()))
                })?;
                ColumnValues::interval(vec![interval; row_count])
            }
            (ConstantExpression::Temporal { span }, DataType::Date) => {
                ColumnValues::date(vec![parse_date(span)?; row_count])
            }
            (ConstantExpression::Temporal { span }, DataType::DateTime) => {
                ColumnValues::datetime(vec![parse_datetime(span)?; row_count])
            }
            (ConstantExpression::Temporal { span }, DataType::Time) => {
                ColumnValues::time(vec![parse_time(span)?; row_count])
            }
            (ConstantExpression::Temporal { span }, DataType::Interval) => {
                ColumnValues::interval(vec![parse_interval(span)?; row_count])
            }

            (ConstantExpression::Undefined { .. }, _) => ColumnValues::Undefined(row_count),

            (_, data_type) => {
                let source_type = match expr {
                    ConstantExpression::Bool { .. } => DataType::Bool,
                    ConstantExpression::Number { .. } => DataType::Float8,
                    ConstantExpression::Text { .. } => DataType::Utf8,
                    ConstantExpression::Temporal { .. } => DataType::DateTime,
                    ConstantExpression::Undefined { .. } => DataType::Undefined,
                };
                return Err(Error(cast::unsupported_cast(expr.span(), source_type, data_type)));
            }
        })
    }

    fn parse_temporal(span: &Span, row_count: usize) -> evaluate::Result<ColumnValues> {
        let fragment = &span.fragment;

        // Route based on character patterns
        if fragment.starts_with('P') || fragment.starts_with('p') {
            // Interval format (ISO 8601 duration)
            let interval = parse_interval(span)?;
            Ok(ColumnValues::interval(vec![interval; row_count]))
        } else if fragment.contains(':') && fragment.contains('-') {
            // DateTime format (contains both : and -)
            let datetime = parse_datetime(span)?;
            Ok(ColumnValues::datetime(vec![datetime; row_count]))
        } else if fragment.contains('-') {
            // Date format with - separators
            let date = parse_date(span)?;
            Ok(ColumnValues::date(vec![date; row_count]))
        } else if fragment.contains(':') {
            // Time format (contains :)
            let time = parse_time(span)?;
            Ok(ColumnValues::time(vec![time; row_count]))
        } else {
            // Unrecognized pattern
            Err(Error(temporal::unrecognized_temporal_pattern(span.clone())))
        }
    }
}
