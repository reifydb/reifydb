// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod number;
mod temporal;
mod text;
mod uuid;

use crate::evaluate::{EvaluationContext, Evaluator};
use number::NumberParser;
use reifydb_core::error::diagnostic::cast;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::value::number::{parse_float, parse_int, parse_uint};
use reifydb_core::{Type, return_error};
use reifydb_rql::expression::ConstantExpression;
use temporal::TemporalParser;
use text::TextParser;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: &ConstantExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        Ok(crate::create_frame_column(
            expr.span().fragment,
            Self::constant_value(&expr, row_count)?
        ))
    }

    pub(crate) fn constant_of(
        &mut self,
        expr: &ConstantExpression,
        target: Type,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let row_count = ctx.take.unwrap_or(ctx.row_count);
        let values = Self::constant_value(&expr, row_count)?;
        let casted_values = {
            let source = values.get_type();
            if source == target {
                values
            } else {
                Self::constant_value_of(&expr, target, row_count)?
            }
        };
        Ok(crate::create_frame_column(expr.span().fragment, casted_values))
    }

    fn constant_value(expr: &ConstantExpression, row_count: usize) -> crate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => match parse_bool(span.clone()) {
                Ok(v) => return Ok(ColumnValues::bool(vec![v; row_count])),
                Err(err) => return_error!(err.diagnostic()),
            },
            ConstantExpression::Number { span } => {
                if span.fragment.contains(".") || span.fragment.contains("e") {
                    return match parse_float(span.clone()) {
                        Ok(v) => Ok(ColumnValues::float8(vec![v; row_count])),
                        Err(err) => return_error!(err.diagnostic()),
                    };
                }

                if let Ok(v) = parse_int::<i8>(span.clone()) {
                    return Ok(ColumnValues::int1(vec![v; row_count]));
                }

                if let Ok(v) = parse_int::<i16>(span.clone()) {
                    return Ok(ColumnValues::int2(vec![v; row_count]));
                }

                if let Ok(v) = parse_int::<i32>(span.clone()) {
                    return Ok(ColumnValues::int4(vec![v; row_count]));
                }

                if let Ok(v) = parse_int::<i64>(span.clone()) {
                    return Ok(ColumnValues::int8(vec![v; row_count]));
                }

                // if parsing as i128 fails and its a negative number, we are maxed out and can stop
                match parse_int::<i128>(span.clone()) {
                    Ok(v) => return Ok(ColumnValues::int16(vec![v; row_count])),
                    Err(err) => {
                        if span.fragment.starts_with("-") {
                            return Err(err);
                        }
                    }
                }

                return match parse_uint::<u128>(span.clone()) {
                    Ok(v) => Ok(ColumnValues::uint16(vec![v; row_count])),
                    Err(err) => {
                        return_error!(err.diagnostic());
                    }
                };
            }
            ConstantExpression::Text { span } => {
                ColumnValues::utf8(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Temporal { span } => {
                TemporalParser::parse_temporal(span.clone(), row_count)?
            }
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }

    fn constant_value_of(
        expr: &ConstantExpression,
        target: Type,
        row_count: usize,
    ) -> crate::Result<ColumnValues> {
        Ok(match (expr, target) {
            (ConstantExpression::Number { span }, target) => {
                NumberParser::from_number(span.clone(), target, row_count)?
            }
            (ConstantExpression::Text { span }, target)
                if target.is_bool()
                    || target.is_number()
                    || target.is_temporal()
                    || target.is_uuid() =>
            {
                TextParser::from_text(span.clone(), target, row_count)?
            }
            (ConstantExpression::Temporal { span }, target) if target.is_temporal() => {
                TemporalParser::from_temporal(span.clone(), target, row_count)?
            }

            (_, target) => {
                let source_type = match expr {
                    ConstantExpression::Bool { .. } => Type::Bool,
                    ConstantExpression::Number { .. } => Type::Float8,
                    ConstantExpression::Text { .. } => Type::Utf8,
                    ConstantExpression::Temporal { .. } => Type::DateTime,
                    ConstantExpression::Undefined { .. } => Type::Undefined,
                };
                return_error!(cast::unsupported_cast(expr.span(), source_type, target));
            }
        })
    }
}
