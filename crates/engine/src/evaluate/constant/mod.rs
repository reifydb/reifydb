// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::error::diagnostic::cast;
use reifydb_core::error::diagnostic::number;
use reifydb_core::error::diagnostic::temporal;
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::value::number::{parse_float, parse_int, parse_uint};
use reifydb_core::value::temporal::{parse_date, parse_datetime, parse_interval, parse_time};
use reifydb_core::{Span, Type};
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
        ty: Type,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value_of(&expr, ty, row_count)?,
        })
    }

    fn constant_value(
        expr: &ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => match parse_bool(span.clone()) {
                Ok(v) => return Ok(ColumnValues::bool(vec![v; row_count])),
                Err(err) => return Err(reifydb_core::Error(err.diagnostic())),
            },
            ConstantExpression::Number { span } => {
                if span.fragment.contains(".") || span.fragment.contains("e") {
                    match parse_float(span.clone()) {
                        Ok(v) => return Ok(ColumnValues::float8(vec![v; row_count])),
                        Err(err) => return Err(reifydb_core::Error(err.diagnostic())),
                    }
                }

                if let Ok(v) = parse_int::<i8>(span.clone()) {
                    ColumnValues::int1(vec![v; row_count])
                } else if let Ok(v) = parse_int::<i16>(span.clone()) {
                    ColumnValues::int2(vec![v; row_count])
                } else if let Ok(v) = parse_int::<i32>(span.clone()) {
                    ColumnValues::int4(vec![v; row_count])
                } else if let Ok(v) = parse_int::<i64>(span.clone()) {
                    ColumnValues::int8(vec![v; row_count])
                } else if let Ok(v) = parse_int::<i128>(span.clone()) {
                    ColumnValues::int16(vec![v; row_count])
                } else {
                    match parse_uint::<u128>(span.clone()) {
                        Ok(v) => ColumnValues::uint16(vec![v; row_count]),
                        Err(err) => {
                            return Err(reifydb_core::Error(err.diagnostic()));
                        }
                    }
                }
            }
            ConstantExpression::Text { span } => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Temporal { span } => Self::parse_temporal(span.clone(), row_count)?,
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }

    fn constant_value_of(
        expr: &ConstantExpression,
        ty: Type,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match (expr, ty) {
            (ConstantExpression::Bool { span }, Type::Bool) => {
                let value = parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))?;
                ColumnValues::bool(vec![value; row_count])
            }

            // Bool to numeric types
            (ConstantExpression::Bool { span }, Type::Float4) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1.0f32
                } else {
                    0.0f32
                };
                ColumnValues::float4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Float8) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1.0f64
                } else {
                    0.0f64
                };
                ColumnValues::float8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Int1) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1i8
                } else {
                    0i8
                };
                ColumnValues::int1(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Int2) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1i16
                } else {
                    0i16
                };
                ColumnValues::int2(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Int4) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1i32
                } else {
                    0i32
                };
                ColumnValues::int4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Int8) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1i64
                } else {
                    0i64
                };
                ColumnValues::int8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Int16) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1i128
                } else {
                    0i128
                };
                ColumnValues::int16(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Uint1) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1u8
                } else {
                    0u8
                };
                ColumnValues::uint1(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Uint2) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1u16
                } else {
                    0u16
                };
                ColumnValues::uint2(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Uint4) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1u32
                } else {
                    0u32
                };
                ColumnValues::uint4(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Uint8) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1u64
                } else {
                    0u64
                };
                ColumnValues::uint8(vec![value; row_count])
            }
            (ConstantExpression::Bool { span }, Type::Uint16) => {
                let value = if parse_bool(span.clone()).map_err(|err| reifydb_core::Error(err.diagnostic()))? {
                    1u128
                } else {
                    0u128
                };
                ColumnValues::uint16(vec![value; row_count])
            }

            (ConstantExpression::Number { span }, ty) => {
                match ty {
                    Type::Bool => match parse_bool(span.clone()) {
                        Ok(v) => ColumnValues::bool(vec![v; row_count]),
                        Err(err) => {
                            return Err(reifydb_core::Error(cast::invalid_boolean(
                                span.clone(),
                                err.diagnostic(),
                            )));
                        }
                    },

                    Type::Float4 => match parse_float::<f32>(span.clone()) {
                        Ok(v) => ColumnValues::float4(vec![v; row_count]),
                        Err(err) => {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                err.diagnostic(),
                            )));
                        }
                    },
                    Type::Float8 => match parse_float::<f64>(span.clone()) {
                        Ok(v) => ColumnValues::float8(vec![v; row_count]),
                        Err(err) => {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                err.diagnostic(),
                            )));
                        }
                    },
                    Type::Int1 => {
                        if let Ok(v) = parse_int::<i8>(span.clone()) {
                            ColumnValues::int1(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                                ColumnValues::int1(vec![truncated as i8; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            match parse_int::<i8>(span.clone()) {
                                Ok(_) => unreachable!(),
                                Err(err) => {
                                    return Err(reifydb_core::Error(cast::invalid_number(
                                        span.clone(),
                                        ty,
                                        err.diagnostic(),
                                    )));
                                }
                            }
                        }
                    }
                    Type::Int2 => {
                        if let Ok(v) = parse_int::<i16>(span.clone()) {
                            ColumnValues::int2(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                                ColumnValues::int2(vec![truncated as i16; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Int4 => {
                        if let Ok(v) = parse_int::<i32>(span.clone()) {
                            ColumnValues::int4(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                                ColumnValues::int4(vec![truncated as i32; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Int8 => {
                        if let Ok(v) = parse_int::<i64>(span.clone()) {
                            ColumnValues::int8(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                                ColumnValues::int8(vec![truncated as i64; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Int16 => {
                        if let Ok(v) = parse_int::<i128>(span.clone()) {
                            ColumnValues::int16(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                                ColumnValues::int16(vec![truncated as i128; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Uint1 => {
                        if let Ok(v) = parse_uint::<u8>(span.clone()) {
                            ColumnValues::uint1(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                                ColumnValues::uint1(vec![truncated as u8; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Uint2 => {
                        if let Ok(v) = parse_uint::<u16>(span.clone()) {
                            ColumnValues::uint2(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                                ColumnValues::uint2(vec![truncated as u16; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Uint4 => {
                        if let Ok(v) = parse_uint::<u32>(span.clone()) {
                            ColumnValues::uint4(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                                ColumnValues::uint4(vec![truncated as u32; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Uint8 => {
                        if let Ok(v) = parse_uint::<u64>(span.clone()) {
                            ColumnValues::uint8(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                                ColumnValues::uint8(vec![truncated as u64; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }
                    Type::Uint16 => {
                        if let Ok(v) = parse_uint::<u128>(span.clone()) {
                            ColumnValues::uint16(vec![v; row_count])
                        } else if let Ok(f) = parse_float::<f64>(span.clone()) {
                            let truncated = f.trunc();
                            if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                                ColumnValues::uint16(vec![truncated as u128; row_count])
                            } else {
                                return Err(reifydb_core::Error(cast::invalid_number(
                                    span.clone(),
                                    ty,
                                    number::number_out_of_range(span.clone(), ty),
                                )));
                            }
                        } else {
                            return Err(reifydb_core::Error(cast::invalid_number(
                                span.clone(),
                                ty,
                                number::invalid_number_format(span.clone(), ty),
                            )));
                        }
                    }

                    _ => {
                        return Err(reifydb_core::Error(cast::unsupported_cast(
                            span.clone(),
                            Type::Float8, // Numbers are treated as float8 by default
                            ty,
                        )));
                    }
                }
            }

            (ConstantExpression::Text { span }, Type::Utf8) => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }

            // Text to numeric types
            (ConstantExpression::Text { span }, Type::Bool) => match parse_bool(span.clone()) {
                Ok(value) => ColumnValues::bool(vec![value; row_count]),
                Err(err) => {
                    return Err(reifydb_core::Error(cast::invalid_boolean(span.clone(), err.diagnostic())));
                }
            },
            (ConstantExpression::Text { span }, Type::Float4) => match parse_float::<f32>(span.clone()) {
                Ok(v) => ColumnValues::float4(vec![v; row_count]),
                Err(err) => {
                    return Err(reifydb_core::Error(cast::invalid_number(
                        span.clone(),
                        Type::Float4,
                        err.diagnostic(),
                    )));
                }
            },
            (ConstantExpression::Text { span }, Type::Float8) => match parse_float::<f64>(span.clone()) {
                Ok(v) => ColumnValues::float8(vec![v; row_count]),
                Err(err) => {
                    return Err(reifydb_core::Error(cast::invalid_number(
                        span.clone(),
                        Type::Float8,
                        err.diagnostic(),
                    )));
                }
            },
            (ConstantExpression::Text { span }, Type::Int1) => {
                ColumnValues::int1(vec![
                    parse_int::<i8>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Int1, e.diagnostic(),)
                    ))?;
                    row_count
                ])
            }

            (ConstantExpression::Text { span }, Type::Int2) => {
                ColumnValues::int2(vec![
                    parse_int::<i16>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Int2, e.diagnostic(),)
                    ))?;
                    row_count
                ])
            }
            (ConstantExpression::Text { span }, Type::Int4) => {
                ColumnValues::int4(vec![
                    parse_int::<i32>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Int4, e.diagnostic(),)
                    ))?;
                    row_count
                ])
            }
            (ConstantExpression::Text { span }, Type::Int8) => {
                ColumnValues::int8(vec![
                    parse_int::<i64>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Int8, e.diagnostic(),)
                    ))?;
                    row_count
                ])
            }

            (ConstantExpression::Text { span }, Type::Int16) => ColumnValues::int16(vec![
                    parse_int::<i128>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Int16, e.diagnostic(),)
                    ))?;
                    row_count
                ]),
            (ConstantExpression::Text { span }, Type::Uint1) => ColumnValues::uint1(vec![
                    parse_uint::<u8>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Uint1, e.diagnostic(),)
                    ))?;
                    row_count
                ]),
            (ConstantExpression::Text { span }, Type::Uint2) => ColumnValues::uint2(vec![
                    parse_uint::<u16>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Uint2, e.diagnostic(),)
                    ))?;
                    row_count
                ]),
            (ConstantExpression::Text { span }, Type::Uint4) => ColumnValues::uint4(vec![
                    parse_uint::<u32>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Uint4, e.diagnostic(),)
                    ))?;
                    row_count
                ]),
            (ConstantExpression::Text { span }, Type::Uint8) => ColumnValues::uint8(vec![
                    parse_uint::<u64>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Uint8, e.diagnostic(),)
                    ))?;
                    row_count
                ]),
            (ConstantExpression::Text { span }, Type::Uint16) => ColumnValues::uint16(vec![
                    parse_uint::<u128>(span.clone()).map_err(|e| reifydb_core::Error(
                        cast::invalid_number(span.clone(), Type::Uint16, e.diagnostic(),)
                    ))?;
                    row_count
                ]),

            (ConstantExpression::Text { span }, Type::Date) => {
                let date = parse_date(span)
                    .map_err(|e| reifydb_core::Error(cast::invalid_temporal(span.clone(), Type::Date, e.0)))?;
                ColumnValues::date(vec![date; row_count])
            }
            (ConstantExpression::Text { span }, Type::DateTime) => {
                let datetime = parse_datetime(span).map_err(|e| {
                    reifydb_core::Error(cast::invalid_temporal(span.clone(), Type::DateTime, e.0))
                })?;
                ColumnValues::datetime(vec![datetime; row_count])
            }
            (ConstantExpression::Text { span }, Type::Time) => {
                let time = parse_time(span)
                    .map_err(|e| reifydb_core::Error(cast::invalid_temporal(span.clone(), Type::Time, e.0)))?;
                ColumnValues::time(vec![time; row_count])
            }
            (ConstantExpression::Text { span }, Type::Interval) => {
                let interval = parse_interval(span).map_err(|e| {
                    reifydb_core::Error(cast::invalid_temporal(span.clone(), Type::Interval, e.0))
                })?;
                ColumnValues::interval(vec![interval; row_count])
            }
            (ConstantExpression::Temporal { span }, Type::Date) => ColumnValues::date(vec![
                    parse_date(span)
                        .map_err(|e| reifydb_core::Error(e.diagnostic()))?;
                    row_count
                ]),
            (ConstantExpression::Temporal { span }, Type::DateTime) => {
                ColumnValues::datetime(vec![
                    parse_datetime(span)
                        .map_err(|e| reifydb_core::Error(e.diagnostic()))?;
                    row_count
                ])
            }
            (ConstantExpression::Temporal { span }, Type::Time) => ColumnValues::time(vec![
                    parse_time(span)
                        .map_err(|e| reifydb_core::Error(e.diagnostic()))?;
                    row_count
                ]),
            (ConstantExpression::Temporal { span }, Type::Interval) => {
                ColumnValues::interval(vec![
                    parse_interval(span)
                        .map_err(|e| reifydb_core::Error(e.diagnostic()))?;
                    row_count
                ])
            }

            (ConstantExpression::Undefined { .. }, _) => ColumnValues::Undefined(row_count),

            (_, ty) => {
                let source_type = match expr {
                    ConstantExpression::Bool { .. } => Type::Bool,
                    ConstantExpression::Number { .. } => Type::Float8,
                    ConstantExpression::Text { .. } => Type::Utf8,
                    ConstantExpression::Temporal { .. } => Type::DateTime,
                    ConstantExpression::Undefined { .. } => Type::Undefined,
                };
                return Err(reifydb_core::Error(cast::unsupported_cast(expr.span(), source_type, ty)));
            }
        })
    }

    fn parse_temporal(span: impl Span, row_count: usize) -> evaluate::Result<ColumnValues> {
        let fragment = span.fragment();

        // Route based on character patterns
        if fragment.starts_with('P') || fragment.starts_with('p') {
            // Interval format (ISO 8601 duration)
            let interval = parse_interval(span).map_err(|e| reifydb_core::Error(e.diagnostic()))?;
            Ok(ColumnValues::interval(vec![interval; row_count]))
        } else if fragment.contains(':') && fragment.contains('-') {
            // DateTime format (contains both : and -)
            let datetime = parse_datetime(span).map_err(|e| reifydb_core::Error(e.diagnostic()))?;
            Ok(ColumnValues::datetime(vec![datetime; row_count]))
        } else if fragment.contains('-') {
            // Date format with - separators
            let date = parse_date(span).map_err(|e| reifydb_core::Error(e.diagnostic()))?;
            Ok(ColumnValues::date(vec![date; row_count]))
        } else if fragment.contains(':') {
            // Time format (contains :)
            let time = parse_time(span).map_err(|e| reifydb_core::Error(e.diagnostic()))?;
            Ok(ColumnValues::time(vec![time; row_count]))
        } else {
            // Unrecognized pattern
            Err(reifydb_core::Error(temporal::unrecognized_temporal_pattern(span.to_owned())))
        }
    }
}
