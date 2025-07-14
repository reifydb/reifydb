// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate;
use crate::evaluate::{EvalutationContext, Evaluator};
use crate::frame::Column;
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &EvalutationContext,
    ) -> evaluate::Result<Column> {
        // FIXME optimization does not apply for prefix expressions, like cast(-2 as int1) at the moment
        match cast.expression.deref() {
            // Optimization: it is a constant value and we now the target data_type, therefore it is possible to create the values directly
            // which means there is no reason to further adjust the column
            Expression::Constant(expr) => self.constant_of(expr, cast.to.data_type, ctx),
            expr => {
                let column = self.evaluate(expr, ctx)?;
                Ok(Column {
                    name: column.name,
                    values: column
                        .values
                        .adjust(cast.to.data_type, ctx, cast.expression.lazy_span())
                        .unwrap(),
                })
            } // FIXME
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::evaluate;
    use crate::evaluate::EvalutationContext;
    use crate::evaluate::Expression;
    use crate::frame::ColumnValues;
    use reifydb_core::DataType;
    use reifydb_core::Span;
    use reifydb_rql::expression::Expression::Prefix;
    use reifydb_rql::expression::{
        CastExpression, ConstantExpression, KindExpression, PrefixExpression, PrefixOperator,
    };
    use ConstantExpression::Number;
    use Expression::{Cast, Constant};

    #[test]
    fn test_cast_integer() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("42") })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int4([42]));
    }

    #[test]
    fn test_cast_negative_integer() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("42") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Int4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int4([-42]));
    }

    #[test]
    fn test_cast_negative_min() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(Span::testing_empty()),
                    expression: Box::new(Constant(Number { span: Span::testing("128") })),
                    span: Span::testing_empty(),
                })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Int1 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::int1([-128]));
    }

    #[test]
    fn test_cast_float_8() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float8([4.2]));
    }

    #[test]
    fn test_cast_float_4() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("4.2") })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float4([4.2]));
    }

    #[test]
    fn test_cast_negative_float_4() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("-1.1") })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Float4 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float4([-1.1]));
    }

    #[test]
    fn test_cast_negative_float_8() {
        let ctx = EvalutationContext::testing();
        let result = evaluate(
            &Cast(CastExpression {
                span: Span::testing_empty(),
                expression: Box::new(Constant(Number { span: Span::testing("-1.1") })),
                to: KindExpression { span: Span::testing_empty(), data_type: DataType::Float8 },
            }),
            &ctx,
        )
        .unwrap();

        assert_eq!(result.values, ColumnValues::float8([-1.1]));
    }
}
