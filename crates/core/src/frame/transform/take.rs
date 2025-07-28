// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::CowVec;
use crate::frame::column::{ColumnQualified, TableQualified};
use crate::frame::{ColumnValues, Frame, FrameColumn};

impl Frame {
    pub fn take(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let data = match &col.values() {
                ColumnValues::Bool(values, bitvec) => ColumnValues::Bool(
                    values.take(n),
                    bitvec.take(n),
                ),
                ColumnValues::Float4(values, bitvec) => ColumnValues::Float4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Float8(values, bitvec) => ColumnValues::Float8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Int1(values, bitvec) => ColumnValues::Int1(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Int2(values, bitvec) => ColumnValues::Int2(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Int4(values, bitvec) => ColumnValues::Int4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Int8(values, bitvec) => ColumnValues::Int8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Int16(values, bitvec) => ColumnValues::Int16(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Utf8(values, bitvec) => ColumnValues::Utf8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uint1(values, bitvec) => ColumnValues::Uint1(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uint2(values, bitvec) => ColumnValues::Uint2(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uint4(values, bitvec) => ColumnValues::Uint4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uint8(values, bitvec) => ColumnValues::Uint8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uint16(values, bitvec) => ColumnValues::Uint16(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Date(values, bitvec) => ColumnValues::Date(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::DateTime(values, bitvec) => ColumnValues::DateTime(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Time(values, bitvec) => ColumnValues::Time(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Interval(values, bitvec) => ColumnValues::Interval(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Undefined(len) => ColumnValues::Undefined(n.min(*len)),
                ColumnValues::RowId(values, bitvec) => ColumnValues::RowId(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uuid4(values, bitvec) => ColumnValues::Uuid4(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
                ColumnValues::Uuid7(values, bitvec) => ColumnValues::Uuid7(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    bitvec.take(n),
                ),
            };

            columns.push(match col.table() {
                Some(table) => FrameColumn::TableQualified(TableQualified {
                    table: table.to_string(),
                    name: col.name().to_string(),
                    values: data,
                }),
                None => FrameColumn::ColumnQualified(ColumnQualified {
                    name: col.name().to_string(),
                    values: data,
                }),
            });
        }

        self.columns = columns;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::column::TableQualified;
    #[test]
    fn test_bool_column() {
        let mut test_instance = Frame::new(vec![TableQualified::bool_with_bitvec(
            "test",
            "flag",
            [true, true, false],
            [false, true, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::bool_with_bitvec([true], [false])
        );
    }

    #[test]
    fn test_float4_column() {
        let mut test_instance = Frame::new(vec![TableQualified::float4_with_bitvec(
            "test",
            "a",
            [1.0, 2.0, 3.0],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::float4_with_bitvec([1.0, 2.0], [true, false])
        );
    }

    #[test]
    fn test_float8_column() {
        let mut test_instance = Frame::new(vec![TableQualified::float8_with_bitvec(
            "test",
            "a",
            [1f64, 2.0, 3.0, 4.0],
            [true, true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::float8_with_bitvec([1.0, 2.0], [true, true])
        );
    }

    #[test]
    fn test_int1_column() {
        let mut test_instance = Frame::new(vec![TableQualified::int1_with_bitvec(
            "test_frame",
            "a",
            [1, 2, 3],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::int1_with_bitvec([1, 2], [true, false])
        );
    }

    #[test]
    fn test_int2_column() {
        let mut test_instance = Frame::new(vec![TableQualified::int2_with_bitvec(
            "test",
            "a",
            [1, 2, 3, 4],
            [true, true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::int2_with_bitvec([1, 2], [true, true])
        );
    }

    #[test]
    fn test_int4_column() {
        let mut test_instance = Frame::new(vec![TableQualified::int4_with_bitvec(
            "test_frame",
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance.columns[0].values(), ColumnValues::int4_with_bitvec([1], [true]));
    }

    #[test]
    fn test_int8_column() {
        let mut test_instance = Frame::new(vec![TableQualified::int8_with_bitvec(
            "test_frame",
            "a",
            [1, 2, 3],
            [false, true, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::int8_with_bitvec([1, 2], [false, true])
        );
    }

    #[test]
    fn test_int16_column() {
        let mut test_instance = Frame::new(vec![TableQualified::int16_with_bitvec(
            "test_frame",
            "a",
            [1, 2],
            [true, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::int16_with_bitvec([1], [true])
        );
    }

    #[test]
    fn test_uint1_column() {
        let mut test_instance = Frame::new(vec![TableQualified::uint1_with_bitvec(
            "test_frame",
            "a",
            [1, 2, 3],
            [false, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::uint1_with_bitvec([1, 2], [false, false])
        );
    }

    #[test]
    fn test_uint2_column() {
        let mut test_instance = Frame::new(vec![TableQualified::uint2_with_bitvec(
            "test_frame",
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::uint2_with_bitvec([1], [true])
        );
    }

    #[test]
    fn test_uint4_column() {
        let mut test_instance = Frame::new(vec![TableQualified::uint4_with_bitvec(
            "test_frame",
            "a",
            [10, 20],
            [false, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::uint4_with_bitvec([10], [false])
        );
    }

    #[test]
    fn test_uint8_column() {
        let mut test_instance = Frame::new(vec![TableQualified::uint8_with_bitvec(
            "test",
            "a",
            [10, 20, 30],
            [true, true, false],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::uint8_with_bitvec([10, 20], [true, true])
        );
    }

    #[test]
    fn test_uint16_column() {
        let mut test_instance = Frame::new(vec![TableQualified::uint16_with_bitvec(
            "test",
            "a",
            [100, 200, 300],
            [true, false, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::uint16_with_bitvec([100], [true])
        );
    }

    #[test]
    fn test_text_column() {
        let mut test_instance = Frame::new(vec![TableQualified::utf8_with_bitvec(
            "test",
            "t",
            ["a", "b", "c"],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::utf8_with_bitvec(["a".to_string(), "b".to_string()], [true, false])
        );
    }

    #[test]
    fn test_undefined_column() {
        let mut test_instance = Frame::new(vec![TableQualified::undefined("test_frame", "u", 3)]);

        test_instance.take(2).unwrap();

        match &test_instance.columns[0].values() {
            ColumnValues::Undefined(size) => {
                assert_eq!(*size, 2);
            }
            _ => panic!("Expected undefined column"),
        }
    }

    #[test]
    fn test_handles_undefined() {
        let mut test_instance = Frame::new(vec![TableQualified::undefined("test_frame", "u", 5)]);

        test_instance.take(3).unwrap();

        match &test_instance.columns[0].values() {
            ColumnValues::Undefined(len) => assert_eq!(*len, 3),
            _ => panic!("Expected Undefined column"),
        }
    }

    #[test]
    fn test_n_larger_than_len_is_safe() {
        let mut test_instance = Frame::new(vec![TableQualified::int2_with_bitvec(
            "test_frame",
            "a",
            [10, 20],
            [true, false],
        )]);

        test_instance.take(10).unwrap();

        assert_eq!(
            *test_instance.columns[0].values(),
            ColumnValues::int2_with_bitvec([10, 20], [true, false])
        );
    }
}
