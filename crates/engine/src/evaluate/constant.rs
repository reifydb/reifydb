// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, EvaluationColumn, Evaluator};
use reifydb_catalog::PolicyError::{Overflow, Underflow};
use reifydb_core::num::{ParseError, parse_float, parse_int, parse_uint};
use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Value, ValueKind};
use reifydb_diagnostic::policy::{
    ColumnOverflow, ColumnUnderflow, column_overflow, column_underflow,
};
use reifydb_frame::ColumnValues;
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: ConstantExpression,
        ctx: &Context,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        if let Some(column) = &ctx.column {
            Self::constant_column(expr, column, row_count)
        } else {
            Self::constant_value(expr, row_count)
        }
    }

    fn constant_column(
        expr: ConstantExpression,
        column: &EvaluationColumn,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        // match column.value {
        //     ValueKind::Int1 => {
        //         match expr {
        //             ConstantExpression::Number(span) => {
        //                 // FIXME handle overflow etc..
        //                 return Ok(ColumnValues::int1(vec![
        //                     span.fragment.parse::<i8>().unwrap();
        //                     1
        //                 ]));
        //             }
        //             _ => unimplemented!(),
        //         }
        //     }
        //     ValueKind::Int2 => {
        //         match expr {
        //             ConstantExpression::Number(span) => {
        //                 // FIXME handle overflow etc..
        //                 return Ok(ColumnValues::int2(vec![
        //                     span.fragment.parse::<i16>().unwrap();
        //                     1
        //                 ]));
        //             }
        //             _ => unimplemented!(),
        //         }
        //     }
        //     _ => unimplemented!()
        // }
        let kind = column.value;
        let value = match expr {
            ConstantExpression::Bool(b) => {
                if kind == ValueKind::Bool {
                    Ok(Value::Bool(b.fragment == "true"))
                } else {
                    Ok(Value::Undefined)
                }
            }
            ConstantExpression::Number(span) => {
                let input = &span.fragment;
                // let _overflow = column.overflow_policy();
                // let _underflow = column.underflow_policy();

                let result = match kind {
                    ValueKind::Float4 => parse_float::<f32>(&input).map(|v| {
                        OrderedF32::try_from(v).map(Value::Float4).unwrap_or(Value::Undefined)
                    }),
                    ValueKind::Float8 => parse_float::<f64>(&input).map(|v| {
                        OrderedF64::try_from(v).map(Value::Float8).unwrap_or(Value::Undefined)
                    }),

                    ValueKind::Int1 => parse_int::<i8>(&input).map(Value::Int1),
                    ValueKind::Int2 => parse_int::<i16>(&input).map(Value::Int2),
                    ValueKind::Int4 => parse_int::<i32>(&input).map(Value::Int4),
                    ValueKind::Int8 => parse_int::<i64>(&input).map(Value::Int8),
                    ValueKind::Int16 => parse_int::<i128>(&input).map(Value::Int16),

                    ValueKind::Uint1 => parse_uint::<u8>(&input).map(Value::Uint1),
                    ValueKind::Uint2 => parse_uint::<u16>(&input).map(Value::Uint2),
                    ValueKind::Uint4 => parse_uint::<u32>(&input).map(Value::Uint4),
                    ValueKind::Uint8 => parse_uint::<u64>(&input).map(Value::Uint8),
                    ValueKind::Uint16 => parse_uint::<u128>(&input).map(Value::Uint16),

                    _ => Ok(Value::Undefined),
                };

                match result {
                    Ok(value) => Ok(value),
                    Err(error) => match error {
                        ParseError::Invalid(_) => Ok(Value::Undefined),
                        ParseError::Overflow(_) => Err(Overflow(column_overflow(ColumnOverflow {
                            span,
                            column: column.name.clone(),
                            value: column.value,
                        }))),
                        ParseError::Underflow(_) => {
                            Err(Underflow(column_underflow(ColumnUnderflow {
                                span,
                                column_name: column.name.clone(),
                                column_value: column.value,
                            })))
                        }
                    },
                }
            }
            ConstantExpression::Text(s) => {
                if kind == ValueKind::String {
                    Ok(Value::String(s.fragment))
                } else {
                    Ok(Value::Undefined)
                }
            }
            ConstantExpression::Undefined(_) => Ok(Value::Undefined),
        };

        Ok(ColumnValues::from(value?))
    }

    fn constant_value(expr: ConstantExpression, row_count: usize) -> evaluate::Result<ColumnValues> {

        Ok(match expr {
            ConstantExpression::Undefined(_) => ColumnValues::Undefined(row_count),
            ConstantExpression::Bool(v) => {
                ColumnValues::bool(vec![v.fragment == "true"; row_count])
            }
            ConstantExpression::Number(s) => {
                let s = s.fragment;
                // FIXME that does not look right..
                // Try parsing in order from most specific to most general
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
                } else if let Ok(v) = s.parse::<u8>() {
                    ColumnValues::uint1(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u16>() {
                    ColumnValues::uint2(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u32>() {
                    ColumnValues::uint4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<u64>() {
                    ColumnValues::uint8(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f32>() {
                    ColumnValues::float4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f64>() {
                    ColumnValues::float8(vec![v; row_count])
                } else {
                    // return Err(evaluate::Error::InvalidConstantNumber(s));
                    unimplemented!()
                }
            }
            ConstantExpression::Text(s) => {
                ColumnValues::string(std::iter::repeat(s.fragment).take(row_count))
            }
        })
    }
}
