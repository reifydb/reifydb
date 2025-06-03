// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, Frame};
use reifydb_core::CowVec;

impl Frame {
    pub fn limit(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let data = match &col.data {
                ColumnValues::Bool(values, valid) => ColumnValues::Bool(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Float4(values, valid) => ColumnValues::Float4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Float8(values, valid) => ColumnValues::Float8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int1(values, valid) => ColumnValues::Int1(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int2(values, valid) => ColumnValues::Int2(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int4(values, valid) => ColumnValues::Int4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int8(values, valid) => ColumnValues::Int8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int16(values, valid) => ColumnValues::Int16(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::String(values, valid) => ColumnValues::String(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Uint1(values, valid) => ColumnValues::Uint1(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Uint2(values, valid) => ColumnValues::Uint2(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Uint4(values, valid) => ColumnValues::Uint4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Uint8(values, valid) => ColumnValues::Uint8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Uint16(values, valid) => ColumnValues::Uint16(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Undefined(len) => ColumnValues::Undefined(n.min(*len)),
            };

            columns.push(Column { name: col.name.clone(), data });
        }

        self.columns = columns;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_bool_column() {
        let mut test_instance = Frame::new(vec![Column::bool_with_validity(
            "flag",
            [true, true, false],
            [false, true, true],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::bool_with_validity([true], [false])
        );
    }

    #[test]
    fn test_float4_column() {
        let mut test_instance = Frame::new(vec![Column::float4_with_validity(
            "a",
            [1.0, 2.0, 3.0],
            [true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::float4_with_validity([1.0, 2.0], [true, false])
        );
    }

    #[test]
    fn test_float8_column() {
        let mut test_instance = Frame::new(vec![Column::float8_with_validity(
            "a",
            [1f64, 2.0, 3.0, 4.0],
            [true, true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::float8_with_validity([1.0, 2.0], [true, true])
        );
    }

    #[test]
    fn test_int1_column() {
        let mut test_instance = Frame::new(vec![Column::int1_with_validity(
            "a",
            [1, 2, 3],
            [true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int1_with_validity([1, 2], [true, false])
        );
    }

    #[test]
    fn test_int2_column() {
        let mut test_instance = Frame::new(vec![Column::int2_with_validity(
            "a",
            [1, 2, 3, 4],
            [true, true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int2_with_validity([1, 2], [true, true])
        );
    }

    #[test]
    fn test_int4_column() {
        let mut test_instance = Frame::new(vec![Column::int4_with_validity(
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int4_with_validity([1], [true])
        );
    }

    #[test]
    fn test_int8_column() {
        let mut test_instance = Frame::new(vec![Column::int8_with_validity(
            "a",
            [1, 2, 3],
            [false, true, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int8_with_validity([1, 2], [false, true])
        );
    }

    #[test]
    fn test_int16_column() {
        let mut test_instance = Frame::new(vec![Column::int16_with_validity(
            "a",
            [1, 2],
            [true, true],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int16_with_validity([1], [true])
        );
    }

    #[test]
    fn test_uint1_column() {
        let mut test_instance = Frame::new(vec![Column::uint1_with_validity(
            "a",
            [1, 2, 3],
            [false, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::uint1_with_validity([1, 2], [false, false])
        );
    }

    #[test]
    fn test_uint2_column() {
        let mut test_instance = Frame::new(vec![Column::uint2_with_validity(
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::uint2_with_validity([1], [true])
        );
    }

    #[test]
    fn test_uint4_column() {
        let mut test_instance = Frame::new(vec![Column::uint4_with_validity(
            "a",
            [10, 20],
            [false, true],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::uint4_with_validity([10], [false])
        );
    }

    #[test]
    fn test_uint8_column() {
        let mut test_instance = Frame::new(vec![Column::uint8_with_validity(
            "a",
            [10, 20, 30],
            [true, true, false],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::uint8_with_validity([10, 20], [true, true])
        );
    }

    #[test]
    fn test_uint16_column() {
        let mut test_instance = Frame::new(vec![Column::uint16_with_validity(
            "a",
            [100, 200, 300],
            [true, false, true],
        )]);

        test_instance.limit(1).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::uint16_with_validity([100], [true])
        );
    }

    #[test]
    fn test_text_column() {
        let mut test_instance =
            Frame::new(vec![Column::string_with_validity("t", ["a", "b", "c"], [true, false, true])]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::string_with_validity(["a".to_string(), "b".to_string()], [true, false])
        );
    }

    #[test]
    fn test_undefined_column() {
        let mut test_instance = Frame::new(vec![Column::undefined("u", 3)]);

        test_instance.limit(2).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Undefined(size) => {
                assert_eq!(*size, 2);
            }
            _ => panic!("Expected undefined column"),
        }
    }

    #[test]
    fn test_handles_undefined() {
        let mut test_instance = Frame::new(vec![Column::undefined("u", 5)]);

        test_instance.limit(3).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Undefined(len) => assert_eq!(*len, 3),
            _ => panic!("Expected Undefined column"),
        }
    }

    #[test]
    fn test_n_larger_than_len_is_safe() {
        let mut test_instance =
            Frame::new(vec![Column::int2_with_validity("a", [10, 20], [true, false])]);

        test_instance.limit(10).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int2_with_validity([10, 20], [true, false])
        );
    }
}
