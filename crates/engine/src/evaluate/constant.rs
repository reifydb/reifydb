// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::ConstantExpression;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        expr: &ConstantExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let row_count = ctx.limit.unwrap_or(ctx.row_count);
        Self::constant_value(&expr, row_count)
    }

    fn constant_value(
        expr: &ConstantExpression,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match expr {
            ConstantExpression::Bool { span } => {
                ColumnValues::bool(vec![span.fragment == "true"; row_count])
            }
            ConstantExpression::Number { span } => {
                let s = &span.fragment;
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
            ConstantExpression::Text { span } => {
                ColumnValues::string(std::iter::repeat(span.fragment.clone()).take(row_count))
            }
            ConstantExpression::Undefined { .. } => ColumnValues::Undefined(row_count),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluate::{EvaluationColumn, Evaluator};
    use crate::frame::ColumnValues;
    use reifydb_catalog::column_policy::ColumnPolicyKind;
    use reifydb_catalog::column_policy::ColumnSaturationPolicy::Error;
    use reifydb_core::ValueKind;
    use reifydb_diagnostic::{Line, Offset, Span};
    use reifydb_rql::expression::ConstantExpression;

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
    fn test_float8() {
        let expr = ConstantExpression::Number { span: make_span("3.14") };
        let col = Evaluator::constant_value(&expr, 2).unwrap();
        assert_eq!(col, ColumnValues::float8(vec![3.14; 2]));
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
            ColumnValues::string(["hello".to_string(), "hello".to_string(), "hello".to_string()])
        );
    }

    #[test]
    fn test_undefined() {
        let expr = ConstantExpression::Undefined { span: make_span("") };
        let col = Evaluator::constant_value(&expr, 2).unwrap();
        assert_eq!(col, ColumnValues::Undefined(2));
    }

    fn make_span(value: &str) -> Span {
        Span { offset: Offset(0), line: Line(1), fragment: value.to_string() }
    }

    fn column_error_policy(name: &str, kind: ValueKind) -> EvaluationColumn {
        EvaluationColumn {
            name: name.to_string(),
            value: kind,
            policies: vec![ColumnPolicyKind::Saturation(Error)],
        }
    }
}
