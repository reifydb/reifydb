// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{Error, EvaluationContext, Evaluator};
use crate::frame::FrameColumn;
use reifydb_core::diagnostic::cast;
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &EvaluationContext,
    ) -> evaluate::Result<FrameColumn> {
        let cast_span = cast.lazy_span();

        // FIXME optimization does not apply for prefix expressions, like cast(-2 as int1) at the moment
        match cast.expression.deref() {
            // Optimization: it is a constant value and we now the target ty, therefore it is possible to create the values directly
            // which means there is no reason to further adjust the column
            Expression::Constant(expr) => self.constant_of(expr, cast.to.ty, ctx),
            expr => {
                let column = self.evaluate(expr, ctx)?;
                Ok(FrameColumn {
                    name: column.name,
                    values: column
                        .values
                        .adjust(cast.to.ty, ctx, cast.expression.lazy_span())
                        .map_err(|e| {
                            Error(cast::invalid_number(cast_span(), cast.to.ty, e.diagnostic()))
                        })?,
                })
            } // FIXME
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::EvaluationContext;
    use crate::evaluate::Expression;
    use crate::evaluate::evaluate;
    use crate::frame::ColumnValues;
    use ConstantExpression::Number;
    use Expression::{Cast, Constant};
    use reifydb_core::Span;
    use reifydb_core::Type;
    use reifydb_rql::expression::Expression::Prefix;
    use reifydb_rql::expression::{
        CastExpression, ConstantExpression, DataTypeExpression, PrefixExpression, PrefixOperator,
    };

    #[test]
    fn test_cast_integer() {
        let ctx = EvaluationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("42") })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Int4 },
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
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("42") })),
                    span: Span::testing_empty(),
                })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Int4 },
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
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("128") })),
                    span: Span::testing_empty(),
                })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Int1 },
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
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Float8 },
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
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Float4 },
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
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("-1.1") })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Float4 },
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
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("-1.1") })),
                to: DataTypeExpression { span: Span::testing_empty(), ty: Type::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float8([-1.1]));
    }
}
