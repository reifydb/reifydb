// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::Columns;
use crate::columnar::{Column, ColumnQualified, TableQualified};

impl Columns {
    pub fn take(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.len());

        for col in self.iter() {
            let data = col.data().take(n);

            columns.push(match col.table() {
                Some(table) => Column::TableQualified(TableQualified {
                    table: table.to_string(),
                    name: col.name().to_string(),
                    data,
                }),
                None => {
                    Column::ColumnQualified(ColumnQualified { name: col.name().to_string(), data })
                }
            });
        }

        *self = Columns::new(columns);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnData;

    #[test]
    fn test_bool_column() {
        let mut test_instance = Columns::new(vec![TableQualified::bool_with_bitvec(
            "test",
            "flag",
            [true, true, false],
            [false, true, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::bool_with_bitvec([true], [false]));
    }

    #[test]
    fn test_float4_column() {
        let mut test_instance = Columns::new(vec![TableQualified::float4_with_bitvec(
            "test",
            "a",
            [1.0, 2.0, 3.0],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance[0].data(),
            ColumnData::float4_with_bitvec([1.0, 2.0], [true, false])
        );
    }

    #[test]
    fn test_float8_column() {
        let mut test_instance = Columns::new(vec![TableQualified::float8_with_bitvec(
            "test",
            "a",
            [1f64, 2.0, 3.0, 4.0],
            [true, true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance[0].data(),
            ColumnData::float8_with_bitvec([1.0, 2.0], [true, true])
        );
    }

    #[test]
    fn test_int1_column() {
        let mut test_instance = Columns::new(vec![TableQualified::int1_with_bitvec(
            "test_columns",
            "a",
            [1, 2, 3],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int1_with_bitvec([1, 2], [true, false]));
    }

    #[test]
    fn test_int2_column() {
        let mut test_instance = Columns::new(vec![TableQualified::int2_with_bitvec(
            "test",
            "a",
            [1, 2, 3, 4],
            [true, true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([1, 2], [true, true]));
    }

    #[test]
    fn test_int4_column() {
        let mut test_instance = Columns::new(vec![TableQualified::int4_with_bitvec(
            "test_columns",
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int4_with_bitvec([1], [true]));
    }

    #[test]
    fn test_int8_column() {
        let mut test_instance = Columns::new(vec![TableQualified::int8_with_bitvec(
            "test_columns",
            "a",
            [1, 2, 3],
            [false, true, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int8_with_bitvec([1, 2], [false, true]));
    }

    #[test]
    fn test_int16_column() {
        let mut test_instance = Columns::new(vec![TableQualified::int16_with_bitvec(
            "test_columns",
            "a",
            [1, 2],
            [true, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int16_with_bitvec([1], [true]));
    }

    #[test]
    fn test_uint1_column() {
        let mut test_instance = Columns::new(vec![TableQualified::uint1_with_bitvec(
            "test_columns",
            "a",
            [1, 2, 3],
            [false, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::uint1_with_bitvec([1, 2], [false, false]));
    }

    #[test]
    fn test_uint2_column() {
        let mut test_instance = Columns::new(vec![TableQualified::uint2_with_bitvec(
            "test_columns",
            "a",
            [1, 2],
            [true, false],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::uint2_with_bitvec([1], [true]));
    }

    #[test]
    fn test_uint4_column() {
        let mut test_instance = Columns::new(vec![TableQualified::uint4_with_bitvec(
            "test_columns",
            "a",
            [10, 20],
            [false, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::uint4_with_bitvec([10], [false]));
    }

    #[test]
    fn test_uint8_column() {
        let mut test_instance = Columns::new(vec![TableQualified::uint8_with_bitvec(
            "test",
            "a",
            [10, 20, 30],
            [true, true, false],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::uint8_with_bitvec([10, 20], [true, true]));
    }

    #[test]
    fn test_uint16_column() {
        let mut test_instance = Columns::new(vec![TableQualified::uint16_with_bitvec(
            "test",
            "a",
            [100, 200, 300],
            [true, false, true],
        )]);

        test_instance.take(1).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::uint16_with_bitvec([100], [true]));
    }

    #[test]
    fn test_text_column() {
        let mut test_instance = Columns::new(vec![TableQualified::utf8_with_bitvec(
            "test",
            "t",
            ["a", "b", "c"],
            [true, false, true],
        )]);

        test_instance.take(2).unwrap();

        assert_eq!(
            *test_instance[0].data(),
            ColumnData::utf8_with_bitvec(["a".to_string(), "b".to_string()], [true, false])
        );
    }

    #[test]
    fn test_undefined_column() {
        let mut test_instance =
            Columns::new(vec![TableQualified::undefined("test_columns", "u", 3)]);

        test_instance.take(2).unwrap();

        match &test_instance[0].data() {
            ColumnData::Undefined(container) => {
                assert_eq!(container.len(), 2);
            }
            _ => panic!("Expected undefined column"),
        }
    }

    #[test]
    fn test_handles_undefined() {
        let mut test_instance =
            Columns::new(vec![TableQualified::undefined("test_columns", "u", 5)]);

        test_instance.take(3).unwrap();

        match &test_instance[0].data() {
            ColumnData::Undefined(container) => assert_eq!(container.len(), 3),
            _ => panic!("Expected Undefined column"),
        }
    }

    #[test]
    fn test_n_larger_than_len_is_safe() {
        let mut test_instance = Columns::new(vec![TableQualified::int2_with_bitvec(
            "test_columns",
            "a",
            [10, 20],
            [true, false],
        )]);

        test_instance.take(10).unwrap();

        assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([10, 20], [true, false]));
    }
}
