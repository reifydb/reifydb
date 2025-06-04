// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
use reifydb_core::{Value, ValueKind};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum ExpressionConstant {
    Undefined,
    Bool(bool),
    // any number
    Number(String),
    // any textual representation can be String, Text, ...
    Text(String),
}

impl Display for ExpressionConstant {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionConstant::Undefined => write!(f, "undefined"),
            ExpressionConstant::Bool(b) => write!(f, "{b}"),
            ExpressionConstant::Number(n) => write!(f, "{n}"),
            ExpressionConstant::Text(s) => write!(f, "\"{s}\""),
        }
    }
}

impl ExpressionConstant {
    pub fn into_value(self, kind: ValueKind) -> Value {
        match self {
            ExpressionConstant::Bool(b) => {
                if kind == ValueKind::Bool {
                    Value::Bool(b)
                } else {
                    Value::Undefined
                }
            }
            ExpressionConstant::Number(n) => match kind {
                ValueKind::Float4 => n
                    .parse::<f32>()
                    .ok()
                    .and_then(|f| OrderedF32::try_from(f).ok())
                    .map(Value::Float4)
                    .unwrap_or(Value::Undefined),
                ValueKind::Float8 => n
                    .parse::<f64>()
                    .ok()
                    .and_then(|f| OrderedF64::try_from(f).ok())
                    .map(Value::Float8)
                    .unwrap_or(Value::Undefined),
                ValueKind::Int1 => {
                    n.parse::<i8>().ok().map(Value::Int1).unwrap_or(Value::Undefined)
                }
                ValueKind::Int2 => {
                    n.parse::<i16>().ok().map(Value::Int2).unwrap_or(Value::Undefined)
                }
                ValueKind::Int4 => {
                    n.parse::<i32>().ok().map(Value::Int4).unwrap_or(Value::Undefined)
                }
                ValueKind::Int8 => {
                    n.parse::<i64>().ok().map(Value::Int8).unwrap_or(Value::Undefined)
                }
                ValueKind::Int16 => {
                    n.parse::<i128>().ok().map(Value::Int16).unwrap_or(Value::Undefined)
                }
                ValueKind::Uint1 => {
                    n.parse::<u8>().ok().map(Value::Uint1).unwrap_or(Value::Undefined)
                }
                ValueKind::Uint2 => {
                    n.parse::<u16>().ok().map(Value::Uint2).unwrap_or(Value::Undefined)
                }
                ValueKind::Uint4 => {
                    n.parse::<u32>().ok().map(Value::Uint4).unwrap_or(Value::Undefined)
                }
                ValueKind::Uint8 => {
                    n.parse::<u64>().ok().map(Value::Uint8).unwrap_or(Value::Undefined)
                }
                ValueKind::Uint16 => {
                    n.parse::<u128>().ok().map(Value::Uint16).unwrap_or(Value::Undefined)
                }
                _ => Value::Undefined,
            },
            ExpressionConstant::Text(s) => {
                if kind == ValueKind::String {
                    Value::String(s)
                } else {
                    Value::Undefined
                }
            }
            ExpressionConstant::Undefined => Value::Undefined,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_true() {
        let expr = ExpressionConstant::Bool(true);
        assert_eq!(expr.into_value(ValueKind::Bool), Value::Bool(true));
    }

    #[test]
    fn test_bool_not_boolean() {
        let expr = ExpressionConstant::Bool(false);
        assert_eq!(expr.into_value(ValueKind::Float4), Value::Undefined);
    }

    #[test]
    fn test_text_string() {
        let expr = ExpressionConstant::Text("hello".to_string());
        assert_eq!(expr.into_value(ValueKind::String), Value::String("hello".to_string()));
    }

    #[test]
    fn test_text_not_text() {
        let expr = ExpressionConstant::Text("ignored".to_string());
        assert_eq!(expr.into_value(ValueKind::Int4), Value::Undefined);
    }

    #[test]
    fn test_number_float4_valid() {
        let expr = ExpressionConstant::Number("1.5".to_string());
        let value = expr.into_value(ValueKind::Float4);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_float4_invalid() {
        let expr = ExpressionConstant::Number("invalid".to_string());
        assert_eq!(expr.into_value(ValueKind::Float4), Value::Undefined);
    }

    #[test]
    fn test_number_float8_valid() {
        let expr = ExpressionConstant::Number("1.5".to_string());
        let value = expr.into_value(ValueKind::Float8);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_float8_invalid() {
        let expr = ExpressionConstant::Number("invalid".to_string());
        assert_eq!(expr.into_value(ValueKind::Float8), Value::Undefined);
    }

    #[test]
    fn test_number_int1_valid() {
        let expr = ExpressionConstant::Number("127".to_string());
        let value = expr.into_value(ValueKind::Int1);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_int1_invalid() {
        let expr = ExpressionConstant::Number("128".to_string());
        assert_eq!(expr.into_value(ValueKind::Int1), Value::Undefined);
    }

    #[test]
    fn test_number_int2_valid() {
        let expr = ExpressionConstant::Number("32767".to_string());
        let value = expr.into_value(ValueKind::Int2);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_int2_invalid() {
        let expr = ExpressionConstant::Number("32768".to_string());
        assert_eq!(expr.into_value(ValueKind::Int2), Value::Undefined);
    }

    #[test]
    fn test_number_int4_valid() {
        let expr = ExpressionConstant::Number("2147483647".to_string());
        let value = expr.into_value(ValueKind::Int4);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_int4_invalid() {
        let expr = ExpressionConstant::Number("2147483648".to_string());
        assert_eq!(expr.into_value(ValueKind::Int4), Value::Undefined);
    }

    #[test]
    fn test_number_int8_valid() {
        let expr = ExpressionConstant::Number("9223372036854775807".to_string());
        let value = expr.into_value(ValueKind::Int8);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_int8_invalid() {
        let expr = ExpressionConstant::Number("9223372036854775808".to_string());
        assert_eq!(expr.into_value(ValueKind::Int8), Value::Undefined);
    }

    #[test]
    fn test_number_int16_valid() {
        let expr =
            ExpressionConstant::Number("170141183460469231731687303715884105727".to_string());
        let value = expr.into_value(ValueKind::Int16);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_int16_invalid() {
        let expr =
            ExpressionConstant::Number("170141183460469231731687303715884105728".to_string());
        assert_eq!(expr.into_value(ValueKind::Int16), Value::Undefined);
    }

    #[test]
    fn test_number_uint1_valid() {
        let expr = ExpressionConstant::Number("255".to_string());
        let value = expr.into_value(ValueKind::Uint1);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_uint1_invalid() {
        let expr = ExpressionConstant::Number("-1".to_string());
        assert_eq!(expr.into_value(ValueKind::Uint1), Value::Undefined);
    }

    #[test]
    fn test_number_uint2_valid() {
        let expr = ExpressionConstant::Number("65535".to_string());
        let value = expr.into_value(ValueKind::Uint2);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_uint2_invalid() {
        let expr = ExpressionConstant::Number("-1".to_string());
        assert_eq!(expr.into_value(ValueKind::Uint2), Value::Undefined);
    }

    #[test]
    fn test_number_uint4_valid() {
        let expr = ExpressionConstant::Number("4294967295".to_string());
        let value = expr.into_value(ValueKind::Uint4);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_uint4_invalid() {
        let expr = ExpressionConstant::Number("-1".to_string());
        assert_eq!(expr.into_value(ValueKind::Uint4), Value::Undefined);
    }

    #[test]
    fn test_number_uint8_valid() {
        let expr = ExpressionConstant::Number("18446744073709551615".to_string());
        let value = expr.into_value(ValueKind::Uint8);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_uint8_invalid() {
        let expr = ExpressionConstant::Number("-1".to_string());
        assert_eq!(expr.into_value(ValueKind::Uint8), Value::Undefined);
    }

    #[test]
    fn test_number_uint16_valid() {
        let expr =
            ExpressionConstant::Number("340282366920938463463374607431768211455".to_string());
        let value = expr.into_value(ValueKind::Uint16);
        assert!(!matches!(value, Value::Undefined));
    }

    #[test]
    fn test_number_uint16_invalid() {
        let expr = ExpressionConstant::Number("-1".to_string());
        assert_eq!(expr.into_value(ValueKind::Uint16), Value::Undefined);
    }

    #[test]
    fn test_undefined_always_returns_undefined() {
        let expr = ExpressionConstant::Undefined;
        assert_eq!(expr.into_value(ValueKind::Int4), Value::Undefined);
    }
}
