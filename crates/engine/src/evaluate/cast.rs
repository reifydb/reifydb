// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        
        // FIXME optimization does not apply for prefix expressions, like cast(-2 as int1) at the moment
        match cast.expression.deref() {
            // Optimization: it is a constant value and we now the target kind, therefore it is possible to create the values directly
            // which means there is no reason to further adjust the column
            Expression::Constant(expr) => self.constant_of(expr, cast.to.kind, ctx),
            expr => Ok(self
                .evaluate(expr, ctx)?
                .adjust_column(cast.to.kind, ctx, cast.expression.lazy_span())
                .unwrap()), // FIXME
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::Context;
    use crate::evaluate::Expression;
    use crate::evaluate::evaluate;
    use crate::frame::ColumnValues;
    use ConstantExpression::Number;
    use Expression::{Cast, Constant};
    use reifydb_core::Kind;
    use reifydb_diagnostic::Span;
    use reifydb_rql::expression::Expression::Prefix;
    use reifydb_rql::expression::{
        CastExpression, ConstantExpression, KindExpression, PrefixExpression, PrefixOperator,
    };

    #[test]
    fn test_cast_integer() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("42") })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::int4([42]));
    }

    #[test]
    fn test_cast_negative_integer() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("42") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::int4([-42]));
    }

    #[test]
    fn test_cast_negative_min() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("128") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Int1 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::int1([-128]));
    }

    #[test]
    fn test_cast_float_8() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::float8([4.2]));
    }

    #[test]
    fn test_cast_float_4() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::float4([4.2]));
    }

    #[test]
    fn test_cast_negative_float_4() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("1.1") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::float4([-1.1]));
    }

    #[test]
    fn test_cast_negative_float_8() {
        let ctx = Context::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("1.1") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), kind: Kind::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result, ColumnValues::float8([-1.100000023841858]));
    }
}
