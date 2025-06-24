// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Error, Evaluator};
use crate::frame::ColumnValues;
use reifydb_core::ValueKind;
use reifydb_diagnostic::Diagnostic;
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: &ConstantExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        // if let Some(column) = &ctx.column {
        //     if let Some(kind) = column.kind {
        //         return self.constant_of(expr, kind, ctx);
        //     }
        // }
        let row_count = ctx.limit.unwrap_or(ctx.row_count);
        Self::constant_value(&expr, row_count)
    }

    pub(crate) fn constant_of(
        &mut self,
        expr: &ConstantExpression,
        kind: ValueKind,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let row_count = ctx.limit.unwrap_or(ctx.row_count);
        Self::constant_value_of(&expr, kind, row_count)
    }

    // FIXME rather than static parsing - it should use the context it is in, this will avoid data wrangling down the line
    fn constant_value(
        expr: &ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }
            ConstantExpression::Number { span } => {
                let s = &span.fragment.replace("_", "");
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
                } else if let Ok(v) = s.parse::<f32>() {
                    if !v.is_finite() {
                        if let Ok(v) = s.parse::<f64>() {
                            return Ok(ColumnValues::float8(vec![v; row_count]));
                        }
                    }
                    ColumnValues::float4(vec![v; row_count])
                } else if let Ok(v) = s.parse::<f64>() {
                    ColumnValues::float8(vec![v; row_count])
                } else {
                    ColumnValues::Undefined(row_count)
                }
            }
            ConstantExpression::Text { span } => {
                ColumnValues::string(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }

    fn constant_value_of(
        expr: &ConstantExpression,
        kind: ValueKind,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match (expr, kind) {
            (ConstantExpression::Bool { span }, ValueKind::Bool) => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }

            (ConstantExpression::Number { span }, kind) => {
                let s = &span.fragment.replace("_", "");
                match kind {
                    ValueKind::Float4 => match s.parse::<f32>() {
                        Ok(v) => ColumnValues::float4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Float8 => match s.parse::<f64>() {
                        Ok(v) => ColumnValues::float8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Int1 => match s.parse::<i8>() {
                        Ok(v) => ColumnValues::int1(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Int2 => match s.parse::<i16>() {
                        Ok(v) => ColumnValues::int2(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Int4 => match s.parse::<i32>() {
                        Ok(v) => ColumnValues::int4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Int8 => match s.parse::<i64>() {
                        Ok(v) => ColumnValues::int8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Int16 => match s.parse::<i128>() {
                        Ok(v) => ColumnValues::int16(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Uint1 => match s.parse::<u8>() {
                        Ok(v) => ColumnValues::uint1(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Uint2 => match s.parse::<u16>() {
                        Ok(v) => ColumnValues::uint2(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Uint4 => match s.parse::<u32>() {
                        Ok(v) => ColumnValues::uint4(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Uint8 => match s.parse::<u64>() {
                        Ok(v) => ColumnValues::uint8(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },
                    ValueKind::Uint16 => match s.parse::<u128>() {
                        Ok(v) => ColumnValues::uint16(vec![v; row_count]),
                        Err(_) => {
                            return Err(Error(Diagnostic::constant_type_mismatch(
                                span.clone(),
                                kind,
                            )));
                        }
                    },

                    _ => {
                        return Err(Error(Diagnostic::constant_type_mismatch(span.clone(), kind)));
                    }
                }
            }

            (ConstantExpression::Text { span }, ValueKind::String) => {
                ColumnValues::string(std::iter::repeat(span.fragment.clone()).take(row_count))
            }

            (ConstantExpression::Undefined { .. }, _) => ColumnValues::Undefined(row_count),

            (_, kind) => {
                return Err(Error(Diagnostic::constant_type_mismatch(expr.span(), kind)));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::EvaluationColumn;
    use ColumnSaturationPolicy::Error;
    use reifydb_catalog::column_policy::{ColumnPolicyKind, ColumnSaturationPolicy};
    use reifydb_core::ValueKind;
    use reifydb_diagnostic::{Line, Offset, Span};

    mod constant_value {
        use crate::evaluate::Evaluator;
        use crate::evaluate::constant::ConstantExpression;
        use crate::evaluate::constant::tests::make_span;
        use crate::frame::ColumnValues;
        use reifydb_diagnostic::Span;

        #[test]
        fn test_bool_true() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            let col = Evaluator::constant_value(&expr, 3).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![true; 3]));
        }

        #[test]
        fn test_bool_false() {
            let expr = ConstantExpression::Bool { span: make_span("false") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![false; 2]));
        }

        #[test]
        fn test_float4() {
            let expr = ConstantExpression::Number { span: make_span("3.14") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::float4(vec![3.14; 2]));
        }

        #[test]
        fn test_float8() {
            let expr =
                ConstantExpression::Number { span: Span::testing(f64::MAX.to_string().as_str()) };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::float8(vec![f64::MAX; 2]));
        }

        #[test]
        fn test_int1() {
            let expr = ConstantExpression::Number { span: make_span("127") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int1(vec![127]));
        }

        #[test]
        fn test_int2() {
            let expr = ConstantExpression::Number { span: make_span("32767") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::int2(vec![32767; 2]));
        }

        #[test]
        fn test_int4() {
            let expr = ConstantExpression::Number { span: make_span("2147483647") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int4(vec![2147483647]));
        }

        #[test]
        fn test_int8() {
            let expr = ConstantExpression::Number { span: make_span("9223372036854775807") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int8(vec![9223372036854775807]));
        }

        #[test]
        fn test_int16() {
            let expr = ConstantExpression::Number {
                span: make_span("170141183460469231731687303715884105727"),
            };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::int16(vec![170141183460469231731687303715884105727i128]));
        }

        #[test]
        fn test_uint16() {
            let expr = ConstantExpression::Number { span: make_span(&u128::MAX.to_string()) };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::uint16(vec![340282366920938463463374607431768211455]));
        }

        #[test]
        fn test_invalid_number_fallback_to_undefined() {
            let expr = ConstantExpression::Number { span: make_span("not_a_number") };
            let col = Evaluator::constant_value(&expr, 1).unwrap();
            assert_eq!(col, ColumnValues::Undefined(1));
        }

        #[test]
        fn test_string() {
            let expr = ConstantExpression::Text { span: make_span("hello") };
            let col = Evaluator::constant_value(&expr, 3).unwrap();
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
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value(&expr, 2).unwrap();
            assert_eq!(col, ColumnValues::Undefined(2));
        }
    }

    mod constant_value_of {
        use crate::evaluate::Evaluator;
        use crate::evaluate::constant::tests::make_span;
        use crate::frame::ColumnValues;
        use reifydb_core::ValueKind;
        use reifydb_rql::expression::ConstantExpression;

        #[test]
        fn test_bool_true() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            let col = Evaluator::constant_value_of(&expr, ValueKind::Bool, 3).unwrap();
            assert_eq!(col, ColumnValues::bool(vec![true; 3]));
        }

        #[test]
        fn test_bool_mismatch() {
            let expr = ConstantExpression::Bool { span: make_span("true") };
            assert!(Evaluator::constant_value_of(&expr, ValueKind::Int1, 1).is_err());
        }

        #[test]
        fn test_int1_ok() {
            number_ok("127", ValueKind::Int1, 2, ColumnValues::int1(vec![127; 2]));
        }
        #[test]
        fn test_int1_type_mismatch() {
            number_type_mismatch("128", ValueKind::Int1);
        }

        #[test]
        fn test_int2_ok() {
            number_ok("32767", ValueKind::Int2, 1, ColumnValues::int2(vec![32767]));
        }
        #[test]
        fn test_int2_type_mismatch() {
            number_type_mismatch("40000", ValueKind::Int2);
        }

        #[test]
        fn test_int4_ok() {
            number_ok("2147483647", ValueKind::Int4, 1, ColumnValues::int4(vec![2147483647]));
        }
        #[test]
        fn test_int4_type_mismatch() {
            number_type_mismatch("9999999999", ValueKind::Int4);
        }

        #[test]
        fn test_int8_ok() {
            number_ok(
                "9223372036854775807",
                ValueKind::Int8,
                1,
                ColumnValues::int8(vec![9223372036854775807]),
            );
        }
        #[test]
        fn test_int8_type_mismatch() {
            number_type_mismatch("999999999999999999999", ValueKind::Int8);
        }

        #[test]
        fn test_int16_ok() {
            number_ok(
                "170141183460469231731687303715884105727",
                ValueKind::Int16,
                1,
                ColumnValues::int16(vec![i128::MAX]),
            );
        }
        #[test]
        fn test_int16_type_mismatch() {
            number_type_mismatch("a", ValueKind::Int16);
        }

        #[test]
        fn test_uint1_ok() {
            number_ok("255", ValueKind::Uint1, 2, ColumnValues::uint1(vec![255; 2]));
        }

        #[test]
        fn test_uint1_type_mismatch() {
            number_type_mismatch("-1", ValueKind::Uint1);
        }

        #[test]
        fn test_uint2_ok() {
            number_ok("65535", ValueKind::Uint2, 1, ColumnValues::uint2(vec![65535]));
        }
        #[test]
        fn test_uint2_type_mismatch() {
            number_type_mismatch("70000", ValueKind::Uint2);
        }

        #[test]
        fn test_uint4_ok() {
            number_ok("4294967295", ValueKind::Uint4, 1, ColumnValues::uint4(vec![4294967295]));
        }
        #[test]
        fn test_uint4_type_mismatch() {
            number_type_mismatch("9999999999", ValueKind::Uint4);
        }

        #[test]
        fn test_uint8_ok() {
            number_ok(
                "18446744073709551615",
                ValueKind::Uint8,
                1,
                ColumnValues::uint8(vec![u64::MAX]),
            );
        }
        #[test]
        fn test_uint8_type_mismatch() {
            number_type_mismatch("-1", ValueKind::Uint8);
        }

        #[test]
        fn test_uint16_ok() {
            number_ok(
                "340282366920938463463374607431768211455",
                ValueKind::Uint16,
                1,
                ColumnValues::uint16(vec![u128::MAX]),
            );
        }
        #[test]
        fn test_uint16_type_mismatch() {
            number_type_mismatch("z", ValueKind::Uint16);
        }

        #[test]
        fn test_float4_ok() {
            number_ok("3.14", ValueKind::Float4, 2, ColumnValues::float4(vec![3.14; 2]));
        }
        #[test]
        fn test_float4_type_mismatch() {
            number_type_mismatch("not_a_float", ValueKind::Float4);
        }

        #[test]
        fn test_float8_ok() {
            number_ok("3.14", ValueKind::Float8, 2, ColumnValues::float8(vec![3.14; 2]));
        }
        #[test]
        fn test_float8_type_mismatch() {
            number_type_mismatch("not_a_float", ValueKind::Float8);
        }

        #[test]
        fn test_text_ok() {
            let expr = ConstantExpression::Text { span: make_span("hello") };
            let col = Evaluator::constant_value_of(&expr, ValueKind::String, 3).unwrap();
            assert_eq!(col, ColumnValues::string(vec!["hello".to_string(); 3]));
        }

        #[test]
        fn test_text_mismatch() {
            let expr = ConstantExpression::Text { span: make_span("text") };
            assert!(Evaluator::constant_value_of(&expr, ValueKind::Int1, 1).is_err());
        }

        #[test]
        fn test_undefined_ok() {
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value_of(&expr, ValueKind::Undefined, 5).unwrap();
            assert_eq!(col, ColumnValues::Undefined(5));
        }

        #[test]
        fn test_undefined_different_kind() {
            let expr = ConstantExpression::Undefined { span: make_span("") };
            let col = Evaluator::constant_value_of(&expr, ValueKind::Float8, 5).unwrap();
            assert_eq!(col, ColumnValues::Undefined(5));
        }

        fn number_ok(expr: &str, kind: ValueKind, row_count: usize, expected: ColumnValues) {
            let expr = ConstantExpression::Number { span: make_span(expr) };
            let result = Evaluator::constant_value_of(&expr, kind, row_count).unwrap();
            assert_eq!(result, expected);
        }

        fn number_type_mismatch(expr: &str, kind: ValueKind) {
            let expr = ConstantExpression::Number { span: make_span(expr) };
            let err = Evaluator::constant_value_of(&expr, kind, 1).unwrap_err();
            assert_eq!(err.diagnostic().code, "EX_001");
        }
    }

    fn make_span(value: &str) -> Span {
        Span { offset: Offset(0), line: Line(1), fragment: value.to_string() }
    }

    fn column_error_policy(name: &str, kind: ValueKind) -> EvaluationColumn {
        EvaluationColumn {
            name: Some(name.to_string()),
            kind: Some(kind),
            policies: vec![ColumnPolicyKind::Saturation(Error)],
        }
    }
}
