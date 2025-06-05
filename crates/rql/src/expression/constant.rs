// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_catalog::{Column, OverflowPolicy, PolicyError, UnderflowPolicy};
use reifydb_core::num::{ParseError, parse_float, parse_int, parse_uint};
use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Value, ValueKind};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum ConstantExpression {
    Undefined,
    Bool(bool),
    // any number
    Number(String),
    // any textual representation can be String, Text, ...
    Text(String),
}

impl Display for ConstantExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConstantExpression::Undefined => write!(f, "undefined"),
            ConstantExpression::Bool(b) => write!(f, "{b}"),
            ConstantExpression::Number(n) => write!(f, "{n}"),
            ConstantExpression::Text(s) => write!(f, "\"{s}\""),
        }
    }
}

impl ConstantExpression {
    pub fn into_column_value(self, column: &Column) -> Result<Value, PolicyError> {
        let kind = column.value;

        match self {
            ConstantExpression::Bool(b) => {
                if kind == ValueKind::Bool {
                    Ok(Value::Bool(b))
                } else {
                    Ok(Value::Undefined)
                }
            }
            ConstantExpression::Number(input) => {
                let overflow = column.overflow_policy();
                let underflow = column.underflow_policy();

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

                if matches!(result, Err(ParseError::Invalid(_))) {
                    return Ok(Value::Undefined);
                }

                result.map_err(|err| match err {
                    ParseError::Overflow(_) => match overflow {
                        OverflowPolicy::Error => PolicyError::Overflow {
                            column: column.name.clone(),
                            value: kind,
                            input,
                        },
                    },
                    ParseError::Underflow(_) => match underflow {
                        UnderflowPolicy::Error => PolicyError::Underflow {
                            column: column.name.clone(),
                            value: kind,
                            input,
                        },
                    },
                    ParseError::Invalid(_) => unreachable!(),
                })
            }
            ConstantExpression::Text(s) => {
                if kind == ValueKind::String {
                    Ok(Value::String(s))
                } else {
                    Ok(Value::Undefined)
                }
            }
            ConstantExpression::Undefined => Ok(Value::Undefined),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::ConstantExpression;
    use reifydb_catalog::{Column, OverflowPolicy, Policy, PolicyError, UnderflowPolicy};
    use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
    use reifydb_core::{Value, ValueKind};

    fn column_error_policy(name: &str, kind: ValueKind) -> Column {
        Column {
            name: name.to_string(),
            value: kind,
            policies: vec![
                Policy::Overflow(OverflowPolicy::Error),
                Policy::Underflow(UnderflowPolicy::Error),
            ],
        }
    }

    #[test]
    fn test_bool() {
        let expr = ConstantExpression::Bool(true);
        let col = column_error_policy("bool_col", ValueKind::Bool);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Bool(true)));
    }

    #[test]
    fn test_float4_error_policy() {
        let expr = ConstantExpression::Number("3.14".into());
        let col = column_error_policy("f4", ValueKind::Float4);
        assert_eq!(
            expr.into_column_value(&col),
            Ok(Value::Float4(OrderedF32::try_from(3.14f32).unwrap()))
        );
    }

    #[test]
    fn test_float4_error_policy_overflow() {
        let expr = ConstantExpression::Number("3.14".into());
        let col = column_error_policy("f4", ValueKind::Float4);
        assert_eq!(
            expr.into_column_value(&col),
            Ok(Value::Float4(OrderedF32::try_from(3.14f32).unwrap()))
        );
    }


    #[test]
    fn test_float8_error_policy() {
        let expr = ConstantExpression::Number("2.718281828".into());
        let col = column_error_policy("f8", ValueKind::Float8);
        assert_eq!(
            expr.into_column_value(&col),
            Ok(Value::Float8(OrderedF64::try_from(2.718281828f64).unwrap()))
        );
    }

    #[test]
    fn test_error_policy_invalid_number_returns_undefined() {
        let expr = ConstantExpression::Number("not_a_number".into());
        let col = column_error_policy("invalid", ValueKind::Int4);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Undefined));
    }

    #[test]
    fn test_error_policy_overflow() {
        let expr = ConstantExpression::Number("128".into());
        let col = column_error_policy("i1", ValueKind::Int1);
        assert!(matches!(
        expr.into_column_value(&col),
        Err(PolicyError::Overflow { column, value: ValueKind::Int1, input }) if column == "i1" && input == "128"
    ));
    }

    #[test]
    fn test_error_policy_underflow() {
        let expr = ConstantExpression::Number("-1".into());
        let col = column_error_policy("u1", ValueKind::Uint1);
        assert!(matches!(
        expr.into_column_value(&col),
        Err(PolicyError::Underflow { column, value: ValueKind::Uint1, input }) if column == "u1" && input == "-1"
    ));
    }

    #[test]
    fn test_int2_error_policy() {
        let expr = ConstantExpression::Number("32767".into());
        let col = column_error_policy("i2", ValueKind::Int2);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Int2(32767)));
    }

    #[test]
    fn test_int4_error_policy() {
        let expr = ConstantExpression::Number("-2147483648".into());
        let col = column_error_policy("i4", ValueKind::Int4);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Int4(-2147483648)));
    }

    #[test]
    fn test_int8_error_policy() {
        let expr = ConstantExpression::Number("9223372036854775807".into());
        let col = column_error_policy("i8", ValueKind::Int8);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Int8(9223372036854775807)));
    }

    #[test]
    fn test_int16_error_policy() {
        let expr = ConstantExpression::Number("-170141183460469231731687303715884105728".into());
        let col = column_error_policy("i16", ValueKind::Int16);
        assert_eq!(
            expr.into_column_value(&col),
            Ok(Value::Int16(-170141183460469231731687303715884105728i128))
        );
    }

    #[test]
    fn test_string_error_policy() {
        let expr = ConstantExpression::Text("hello world".into());
        let col = column_error_policy("txt", ValueKind::String);
        assert_eq!(expr.into_column_value(&col), Ok(Value::String("hello world".into())));
    }

    #[test]
    fn test_uint1_error_policy() {
        let expr = ConstantExpression::Number("255".into());
        let col = column_error_policy("u1", ValueKind::Uint1);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Uint1(255)));
    }

    #[test]
    fn test_uint2_error_policy() {
        let expr = ConstantExpression::Number("65535".into());
        let col = column_error_policy("u2", ValueKind::Uint2);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Uint2(65535)));
    }

    #[test]
    fn test_uint4_error_policy() {
        let expr = ConstantExpression::Number("4294967295".into());
        let col = column_error_policy("u4", ValueKind::Uint4);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Uint4(4294967295)));
    }

    #[test]
    fn test_uint8_error_policy() {
        let expr = ConstantExpression::Number("18446744073709551615".into());
        let col = column_error_policy("u8", ValueKind::Uint8);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Uint8(18446744073709551615)));
    }

    #[test]
    fn test_uint16_error_policy() {
        let expr = ConstantExpression::Number("340282366920938463463374607431768211455".into());
        let col = column_error_policy("u16", ValueKind::Uint16);
        assert_eq!(
            expr.into_column_value(&col),
            Ok(Value::Uint16(340282366920938463463374607431768211455u128))
        );
    }

    #[test]
    fn test_undefined() {
        let expr = ConstantExpression::Number("123".into());
        let col = column_error_policy("undef", ValueKind::Undefined);
        assert_eq!(expr.into_column_value(&col), Ok(Value::Undefined));
    }
}
