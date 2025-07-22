// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod number;
mod temporal;
mod text;

use crate::evaluate::{EvaluationContext, Evaluator};
use crate::frame::{ColumnValues, FrameColumn};
use number::NumberParser;
use reifydb_core::error::diagnostic::cast;
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
        Ok(FrameColumn {
            name: expr.span().fragment,
            values: Self::constant_value(&expr, row_count)?,
        })
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
            // Only use ColumnValues.cast() for cases where we know it works well
            if source == target {
                values
            } else if (source.is_number() && target.is_number())
                || (source.is_number() && target.is_bool())
                || (source == Type::Utf8 && target.is_temporal())
                || (source == Type::Bool && target == Type::Bool)
                || (source == Type::Utf8 && target == Type::Utf8)
                || (source == Type::Undefined)
                || (source == Type::Bool && target.is_number())
            {
                // These cases work well with ColumnValues.cast() and provide good diagnostics
                match values.cast(target, ctx, || expr.span().into()) {
                    Ok(casted) => casted,
                    Err(_) => {
                        // Even in "good" cases, fall back if cast fails
                        Self::constant_value_of(&expr, target, row_count)?
                    }
                }
            } else {
                // For all other cases, use the original detailed logic
                Self::constant_value_of(&expr, target, row_count)?
            }
        };
        Ok(FrameColumn { name: expr.span().fragment, values: casted_values })
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
                            return_error!(err.diagnostic());
                        }
                    }
                }
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
                if target.is_bool() || target.is_number() || target.is_temporal() =>
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
