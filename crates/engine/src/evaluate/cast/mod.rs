// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod blob;
pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use crate::columnar::{Column, ColumnData, ColumnQualified, TableQualified};
use crate::evaluate::{Convert, Demote, EvaluationContext, Evaluator, Promote};
use reifydb_core::result::error::diagnostic::cast;
use reifydb_core::{OwnedSpan, Type, err, error};
use reifydb_rql::expression::{CastExpression, Expression};
use std::ops::Deref;

impl Evaluator {
    pub(crate) fn cast(
        &mut self,
        cast: &CastExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let cast_span = cast.lazy_span();

        // FIXME optimization does not apply for prefix expressions, like cast(-2 as int1) at the moment
        match cast.expression.deref() {
            // Optimization: it is a constant value and we now the target ty, therefore it is possible to create the data directly
            // which means there is no reason to further adjust the column
            Expression::Constant(expr) => self.constant_of(expr, cast.to.ty, ctx),
            expr => {
                let column = self.evaluate(expr, ctx)?;

                let casted =
                    cast_column_data(&column.data(), cast.to.ty, ctx, cast.expression.lazy_span())
                        .map_err(|e| {
                            error!(cast::invalid_number(cast_span(), cast.to.ty, e.diagnostic()))
                        })?;

                Ok(match column.table() {
                    Some(table) => Column::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: casted,
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: casted,
                    }),
                })
            }
        }
    }
}

pub fn cast_column_data(
    data: &ColumnData,
    target: Type,
    ctx: impl Promote + Demote + Convert,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
    if let ColumnData::Undefined(container) = data {
        let mut result = ColumnData::with_capacity(target, container.len());
        for _ in 0..container.len() {
            result.push_undefined();
        }
        return Ok(result);
    }
    let source_type = data.get_type();
    match (source_type, target) {
        _ if target == source_type => Ok(data.clone()),
        (_, target) if target.is_number() => number::to_number(data, target, ctx, span),
        (_, target) if target.is_blob() => blob::to_blob(data, span),
        (_, target) if target.is_bool() => boolean::to_boolean(data, span),
        (_, target) if target.is_utf8() => text::to_text(data, span),
        (_, target) if target.is_temporal() => temporal::to_temporal(data, target, span),
        (_, target) if target.is_uuid() => uuid::to_uuid(data, target, span),
        (source, target) if source.is_uuid() || target.is_uuid() => {
            uuid::to_uuid(data, target, span)
        }
        _ => {
            err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::columnar::ColumnData;
    use crate::evaluate::EvaluationContext;
    use crate::evaluate::Expression;
    use crate::evaluate::evaluate;
    use ConstantExpression::Number;
    use Expression::{Cast, Constant};
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

        assert_eq!(*result.data(), ColumnData::int4([42]));
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

        assert_eq!(*result.data(), ColumnData::int4([-42]));
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

        assert_eq!(*result.data(), ColumnData::int1([-128]));
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

        assert_eq!(*result.data(), ColumnData::float8([4.2]));
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

        assert_eq!(*result.data(), ColumnData::float4([4.2]));
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

        assert_eq!(*result.data(), ColumnData::float4([-1.1]));
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

        assert_eq!(*result.data(), ColumnData::float8([-1.1]));
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

        assert_eq!(*result.data(), ColumnData::bool([false]));
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
