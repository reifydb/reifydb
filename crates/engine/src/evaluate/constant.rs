// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::context::EvaluationColumn;
use crate::evaluate::{Context, Evaluator};
use reifydb_catalog::ColumnPolicyError::{Overflow, Underflow};
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
    ) -> evaluate::Result<ColumnValues> {
        let row_count = ctx.row_count_or_one();

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
        let kind = column.value;
        let value = match expr {
            ConstantExpression::Bool(b) => match kind {
                ValueKind::Bool => Ok(Value::Bool(b.fragment == "true")),
                ValueKind::String => Ok(Value::String(b.fragment)),
                _ => Ok(Value::Undefined),
            },
            ConstantExpression::Number(span) => {
                let input = &span.fragment;

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

        Ok(ColumnValues::from_many(value?, row_count))
    }

    fn constant_value(
        expr: ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool(v) => {
                ColumnValues::bool(vec![v.fragment == "true"; row_count])
            }
            ConstantExpression::Number(s) => {
                let s = s.fragment;
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
                } else if let Ok(v) = s.parse::<u128>() {
                    ColumnValues::uint16(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f64>() {
                    ColumnValues::float8(vec![v; row_count])
                } else {
                    ColumnValues::Undefined(row_count)
                }
            }
            ConstantExpression::Text(s) => {
                ColumnValues::string(std::iter::repeat(s.fragment).take(row_count))
            }
            ConstantExpression::Undefined(_) => ColumnValues::Undefined(row_count),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::EvaluationColumn;
    use reifydb_catalog::{ColumnOverflowPolicy, ColumnPolicy, ColumnUnderflowPolicy};
    use reifydb_core::ValueKind;
    use reifydb_diagnostic::{Line, Offset, Span};

    mod constant_column {
        use crate::evaluate::constant::tests::{column_error_policy, make_span};
        use crate::evaluate::{Error, Evaluator};
        use reifydb_core::ValueKind;
        use reifydb_diagnostic::DiagnosticColumn;
        use reifydb_frame::ColumnValues;
        use reifydb_rql::expression::ConstantExpression;

        #[test]
        fn test_bool() {
            let expr = ConstantExpression::Bool(make_span("true"));
            let col = column_error_policy("bool_col", ValueKind::Bool);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::bool([true])));
        }

        #[test]
        fn test_bool_expression_but_expected_text() {
            let expr = ConstantExpression::Bool(make_span("true"));
            let col = column_error_policy("bool_col", ValueKind::String);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::string(["true".to_string()])));
        }

        #[test]
        fn test_float4_error_policy() {
            let expr = ConstantExpression::Number(make_span("3.14"));
            let col = column_error_policy("f4", ValueKind::Float4);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::float4([3.14])));
        }

        #[test]
        fn test_float8_error_policy() {
            let expr = ConstantExpression::Number(make_span("2.718281828"));
            let col = column_error_policy("f8", ValueKind::Float8);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::float8([2.718281828f64])));
        }

        #[test]
        fn test_int1_error_policy() {
            let expr = ConstantExpression::Number(make_span("127"));
            let col = column_error_policy("i2", ValueKind::Int1);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::int1([127])));
        }

        #[test]
        fn test_int2_error_policy() {
            let expr = ConstantExpression::Number(make_span("32767"));
            let col = column_error_policy("i2", ValueKind::Int2);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::int2([32767])));
        }

        #[test]
        fn test_int4_error_policy() {
            let expr = ConstantExpression::Number(make_span("-2147483648"));
            let col = column_error_policy("i4", ValueKind::Int4);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::int4([-2147483648])));
        }

        #[test]
        fn test_int8_error_policy() {
            let expr = ConstantExpression::Number(make_span("9223372036854775807"));
            let col = column_error_policy("i8", ValueKind::Int8);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::int8([9223372036854775807])));
        }

        #[test]
        fn test_int16_error_policy() {
            let expr =
                ConstantExpression::Number(make_span("-170141183460469231731687303715884105728"));
            let col = column_error_policy("i16", ValueKind::Int16);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(
                result,
                Ok(ColumnValues::int16([-170141183460469231731687303715884105728i128]))
            );
        }

        #[test]
        fn test_uint1_error_policy() {
            let expr = ConstantExpression::Number(make_span("255"));
            let col = column_error_policy("u1", ValueKind::Uint1);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::uint1([255])));
        }

        #[test]
        fn test_uint2_error_policy() {
            let expr = ConstantExpression::Number(make_span("65535"));
            let col = column_error_policy("u2", ValueKind::Uint2);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::uint2([65535])));
        }

        #[test]
        fn test_uint4_error_policy() {
            let expr = ConstantExpression::Number(make_span("4294967295"));
            let col = column_error_policy("u4", ValueKind::Uint4);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::uint4([4294967295])));
        }

        #[test]
        fn test_uint8_error_policy() {
            let expr = ConstantExpression::Number(make_span("18446744073709551615"));
            let col = column_error_policy("u8", ValueKind::Uint8);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(result, Ok(ColumnValues::uint8([18446744073709551615])));
        }

        #[test]
        fn test_uint16_error_policy() {
            let expr =
                ConstantExpression::Number(make_span("340282366920938463463374607431768211455"));
            let col = column_error_policy("u16", ValueKind::Uint16);
            let result = Evaluator::constant_column(expr, &col, 1);
            assert_eq!(
                result,
                Ok(ColumnValues::uint16([340282366920938463463374607431768211455u128]))
            );
        }

        #[test]
        fn test_error_policy_overflow() {
            let expr = ConstantExpression::Number(make_span("128"));
            let col = column_error_policy("i1", ValueKind::Int1);
            let result = Evaluator::constant_column(expr, &col, 1);
            let Err(Error(diagnostic)) = result else { unreachable!() };

            assert_eq!(diagnostic.code, "PO0001");
            assert_eq!(
                diagnostic.column,
                Some(DiagnosticColumn { name: "i1".to_string(), value: ValueKind::Int1 })
            );

            assert_eq!(
                diagnostic.label.unwrap().as_str(),
                "value `128` does not fit into `INT1` (range: -128 to 127)"
            );
        }

        #[test]
        fn test_error_policy_underflow() {
            let expr = ConstantExpression::Number(make_span("-1"));
            let col = column_error_policy("u1", ValueKind::Uint1);
            let result = Evaluator::constant_column(expr, &col, 1);
            let Err(Error(diagnostic)) = result else { unreachable!() };

            assert_eq!(diagnostic.code, "PO0002");
            assert_eq!(
                diagnostic.column,
                Some(DiagnosticColumn { name: "u1".to_string(), value: ValueKind::Uint1 })
            );

            assert_eq!(
                diagnostic.label.unwrap().as_str(),
                "value `-1` does not fit into `UINT1` (range: 0 to 255)"
            );
        }
    }

    mod constant_value {
        use crate::evaluate::Evaluator;
        use crate::evaluate::constant::tests::make_span;
        use reifydb_frame::ColumnValues;
        use reifydb_rql::expression::ConstantExpression;

        #[test]
        fn test_bool_true() {
            let expr = ConstantExpression::Bool(make_span("true"));
            let col = Evaluator::constant_value(expr, 3).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![true; 3]));
        }

        #[test]
        fn test_bool_false() {
            let expr = ConstantExpression::Bool(make_span("false"));
            let col = Evaluator::constant_value(expr, 2).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![false; 2]));
        }

        #[test]
        fn test_int1() {
            let expr = ConstantExpression::Number(make_span("127"));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int1(vec![127]));
        }

        #[test]
        fn test_int2() {
            let expr = ConstantExpression::Number(make_span("32767"));
            let col = Evaluator::constant_value(expr, 2).unwrap();
            assert_eq!(col, ColumnValues::int2(vec![32767; 2]));
        }

        #[test]
        fn test_int4() {
            let expr = ConstantExpression::Number(make_span("2147483647"));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int4(vec![2147483647]));
        }

        #[test]
        fn test_int8() {
            let expr = ConstantExpression::Number(make_span("9223372036854775807"));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int8(vec![9223372036854775807]));
        }

        #[test]
        fn test_int16() {
            let expr =
                ConstantExpression::Number(make_span("170141183460469231731687303715884105727"));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int16(vec![170141183460469231731687303715884105727i128]));
        }

        #[test]
        fn test_uint16() {
            let expr = ConstantExpression::Number(make_span(&u128::MAX.to_string()));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::uint16(vec![340282366920938463463374607431768211455]));
        }

        #[test]
        fn test_float8() {
            let expr = ConstantExpression::Number(make_span("3.14"));
            let col = Evaluator::constant_value(expr, 2).unwrap();
            assert_eq!(col, ColumnValues::float8(vec![3.14; 2]));
        }

        #[test]
        fn test_invalid_number_fallback_to_undefined() {
            let expr = ConstantExpression::Number(make_span("not_a_number"));
            let col = Evaluator::constant_value(expr, 1).unwrap();
            assert_eq!(col, ColumnValues::Undefined(1));
        }

        #[test]
        fn test_string() {
            let expr = ConstantExpression::Text(make_span("hello"));
            let col = Evaluator::constant_value(expr, 3).unwrap();
            assert_eq!(
                col,
                ColumnValues::string([
                    "hello".to_string(),
                    "hello".to_string(),
                    "hello".to_string()
                ])
            );
        }

        #[test]
        fn test_undefined() {
            let expr = ConstantExpression::Undefined(make_span(""));
            let col = Evaluator::constant_value(expr, 2).unwrap();
            assert_eq!(col, ColumnValues::Undefined(2));
        }
    }

    fn make_span(value: &str) -> Span {
        Span { offset: Offset(0), line: Line(1), fragment: value.to_string() }
    }

    fn column_error_policy(name: &str, kind: ValueKind) -> EvaluationColumn {
        EvaluationColumn {
            name: name.to_string(),
            value: kind,
            policies: vec![
                ColumnPolicy::Overflow(ColumnOverflowPolicy::Error),
                ColumnPolicy::Underflow(ColumnUnderflowPolicy::Error),
            ],
        }
    }
}
