// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::Evaluator;
use reifydb_core::Value;
use reifydb_frame::ColumnValues;

impl Evaluator {
    pub(crate) fn constant(
        &mut self,
        value: Value,
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(match value {
            Value::Bool(v) => ColumnValues::bool(vec![v; row_count]),
            Value::Float4(v) => ColumnValues::float4(vec![v.value(); row_count]),
            Value::Float8(v) => ColumnValues::float8(vec![v.value(); row_count]),
            Value::Int1(v) => ColumnValues::int1(vec![v; row_count]),
            Value::Int2(v) => ColumnValues::int2(vec![v; row_count]),
            Value::Int4(v) => ColumnValues::int4(vec![v; row_count]),
            Value::Int8(v) => ColumnValues::int8(vec![v; row_count]),
            Value::Int16(v) => ColumnValues::int16(vec![v; row_count]),
            Value::Uint1(v) => ColumnValues::uint1(vec![v; row_count]),
            Value::Uint2(v) => ColumnValues::uint2(vec![v; row_count]),
            Value::Uint4(v) => ColumnValues::uint4(vec![v; row_count]),
            Value::Uint8(v) => ColumnValues::uint8(vec![v; row_count]),
            Value::Uint16(v) => ColumnValues::uint16(vec![v; row_count]),
            Value::String(v) => ColumnValues::string(std::iter::repeat(v).take(row_count)),
            Value::Undefined => ColumnValues::Undefined(row_count),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use reifydb_core::Value;

    #[test]
    fn test_bool() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Bool(true), 3).unwrap();
        assert_eq!(col, ColumnValues::bool(vec![true, true, true]));
    }

    #[test]
    fn test_float4() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::float4(1.25), 2).unwrap();
        assert_eq!(col, ColumnValues::float4(vec![1.25, 1.25]));
    }

    #[test]
    fn test_float8() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::float8(3.14), 2).unwrap();
        assert_eq!(col, ColumnValues::float8(vec![3.14, 3.14]));
    }

    #[test]
    fn test_int1() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Int1(-7), 4).unwrap();
        assert_eq!(col, ColumnValues::int1(vec![-7; 4]));
    }

    #[test]
    fn test_int2() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Int2(42), 3).unwrap();
        assert_eq!(col, ColumnValues::int2(vec![42; 3]));
    }

    #[test]
    fn test_int4() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Int4(2024), 2).unwrap();
        assert_eq!(col, ColumnValues::int4(vec![2024, 2024]));
    }

    #[test]
    fn test_int8() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Int8(-123456), 2).unwrap();
        assert_eq!(col, ColumnValues::int8(vec![-123456; 2]));
    }

    #[test]
    fn test_int16() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Int16(1_000_000_000_000), 2).unwrap();
        assert_eq!(col, ColumnValues::int16(vec![1_000_000_000_000; 2]));
    }

    #[test]
    fn test_uint1() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Uint1(8), 3).unwrap();
        assert_eq!(col, ColumnValues::uint1(vec![8; 3]));
    }

    #[test]
    fn test_uint2() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Uint2(16), 3).unwrap();
        assert_eq!(col, ColumnValues::uint2(vec![16; 3]));
    }

    #[test]
    fn test_uint4() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Uint4(1024), 3).unwrap();
        assert_eq!(col, ColumnValues::uint4(vec![1024; 3]));
    }

    #[test]
    fn test_uint8() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Uint8(1_000_000), 3).unwrap();
        assert_eq!(col, ColumnValues::uint8(vec![1_000_000; 3]));
    }

    #[test]
    fn test_uint16() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Uint16(1_000_000_000_000), 2).unwrap();
        assert_eq!(col, ColumnValues::uint16(vec![1_000_000_000_000; 2]));
    }

    #[test]
    fn test_string() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::String("hello".into()), 3).unwrap();
        assert_eq!(col, ColumnValues::string(vec!["hello".to_string(); 3]));
    }

    #[test]
    fn test_undefined() {
        let mut eval = Evaluator::default();
        let col = eval.constant(Value::Undefined, 5).unwrap();
        assert_eq!(col, ColumnValues::Undefined(5));
    }
}
