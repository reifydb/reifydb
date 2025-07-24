// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use crate::evaluate::{Convert, Demote, EvaluationContext, Evaluator, Promote};
use reifydb_core::error::diagnostic::cast;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use reifydb_core::{OwnedSpan, Type, err, error};
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let cast_span = cast.lazy_span();

        // FIXME optimization does not apply for prefix expressions, like cast(-2 as int1) at the moment
        match cast.expression.deref() {
            // Optimization: it is a constant value and we now the target ty, therefore it is possible to create the values directly
            // which means there is no reason to further adjust the column
            Expression::Constant(expr) => self.constant_of(expr, cast.to.ty, ctx),
            expr => {
                let column = self.evaluate(expr, ctx)?;

                // Re-enable cast functionality using the moved cast module
                Ok(FrameColumn {
                    name: column.name,
                    values: cast_column_values(
                        &column.values,
                        cast.to.ty,
                        ctx,
                        cast.expression.lazy_span(),
                    )
                    .map_err(|e| {
                        error!(cast::invalid_number(cast_span(), cast.to.ty, e.diagnostic()))
                    })?,
                })
            }
        }
    }
}

pub fn cast_column_values(
    values: &ColumnValues,
    target: Type,
    ctx: impl Promote + Demote + Convert,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnValues> {
    if let ColumnValues::Undefined(size) = values {
        let mut result = ColumnValues::with_capacity(target, *size);
        for _ in 0..*size {
            result.push_undefined();
        }
        return Ok(result);
    }
    let source_type = values.get_type();
    match (source_type, target) {
        _ if target == source_type => Ok(values.clone()),
        (_, target) if target.is_number() => number::to_number(values, target, ctx, span),
        (_, target) if target.is_bool() => boolean::to_boolean(values, span),
        (_, target) if target.is_utf8() => text::to_text(values, span),
        (_, target) if target.is_temporal() => temporal::to_temporal(values, target, span),
        (_, target) if target.is_uuid() => uuid::to_uuid(values, target, span),
        (source, target) if source.is_uuid() || target.is_uuid() => uuid::to_uuid(values, target, span),
        _ => {
            err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::EvaluationContext;
    use crate::evaluate::Expression;
    use crate::evaluate::evaluate;
    use ConstantExpression::Number;
    use Expression::{Cast, Constant};
    use reifydb_core::frame::ColumnValues;
    use reifydb_core::{OwnedSpan, Type};
    use reifydb_rql::expression::Expression::Prefix;
    use reifydb_rql::expression::{
        CastExpression, ConstantExpression, DataTypeExpression, PrefixExpression, PrefixOperator,
    };

    #[test]
    fn test_cast_integer() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(Number { span: OwnedSpan::testing("42") })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int4([42]));
    }

    #[test]
    fn test_cast_negative_integer() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(OwnedSpan::testing_empty()),
                    expression: Box::new(Constant(Number { span: OwnedSpan::testing("42") })),
                    span: OwnedSpan::testing_empty(),
                })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int4([-42]));
    }

    #[test]
    fn test_cast_negative_min() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(OwnedSpan::testing_empty()),
                    expression: Box::new(Constant(Number { span: OwnedSpan::testing("128") })),
                    span: OwnedSpan::testing_empty(),
                })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Int1 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int1([-128]));
    }

    #[test]
    fn test_cast_float_8() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(Number { span: OwnedSpan::testing("4.2") })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float8([4.2]));
    }

    #[test]
    fn test_cast_float_4() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(Number { span: OwnedSpan::testing("4.2") })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float4([4.2]));
    }

    #[test]
    fn test_cast_negative_float_4() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(Number { span: OwnedSpan::testing("-1.1") })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float4([-1.1]));
    }

    #[test]
    fn test_cast_negative_float_8() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(Number { span: OwnedSpan::testing("-1.1") })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float8([-1.1]));
    }

    #[test]
    fn test_cast_string_to_bool() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(ConstantExpression::Text {
                    span: OwnedSpan::testing("0"),
                })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Bool },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::bool([false]));
    }

    #[test]
    fn test_cast_string_neg_one_to_bool_should_fail() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(ConstantExpression::Text {
                    span: OwnedSpan::testing("-1"),
                })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Bool },
            }),
            &ctx,
        );

        assert!(result.is_err());

        // Check that the error is the expected CAST_004 (invalid_boolean) error
        let err = result.unwrap_err();
        let diagnostic = err.0;
        assert_eq!(diagnostic.code, "CAST_004");
        assert!(diagnostic.cause.is_some());
        let cause = diagnostic.cause.unwrap();
        assert_eq!(cause.code, "BOOLEAN_003"); // invalid_number_boolean
    }

    #[test]
    fn test_cast_bool_to_date_should_fail() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Constant(ConstantExpression::Bool {
                    span: OwnedSpan::testing("true"),
                })),
                to: DataTypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Date },
            }),
            &ctx,
        );

        assert!(result.is_err());

        // Check that the error is the expected CAST_001 (unsupported_cast) error
        let err = result.unwrap_err();
        let diagnostic = err.0;
        assert_eq!(diagnostic.code, "CAST_001");
    }
}
