// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, DataFrame};
use base::CowVec;

impl DataFrame {
    pub fn limit(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let data = match &col.data {
                ColumnValues::Float8(values, valid) => ColumnValues::Float8(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Int2(values, valid) => ColumnValues::Int2(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Text(values, valid) => ColumnValues::Text(
                    CowVec::new(values[..n.min(values.len())].to_vec()),
                    CowVec::new(valid[..n.min(valid.len())].to_vec()),
                ),
                ColumnValues::Bool(values, valid) => ColumnValues::Bool(
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
    fn test_truncates_float8_column() {
        let mut test_instance = DataFrame::new(vec![Column::float8_with_validity(
            "a",
            [1f64, 2.0, 3.0, 4.0],
            [true, true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(test_instance.columns[0].data, ColumnValues::float8([1.0, 2.0]));
    }

    #[test]
    fn test_truncates_int2_column() {
        let mut test_instance = DataFrame::new(vec![Column::int2_with_validity(
            "a",
            [1, 2, 3, 4],
            [true, true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(test_instance.columns[0].data, ColumnValues::int2([1, 2]));
    }

    #[test]
    fn test_limit_truncates_text_column() {
        let mut test_instance = DataFrame::new(vec![Column::text_with_validity(
            "t",
            ["a", "b", "c"],
            [true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::text_with_validity(["a".to_string(), "b".to_string()], [true, false])
        );
    }

    #[test]
    fn test_limit_truncates_bool_column() {
        let mut test_instance = DataFrame::new(vec![Column::bool_with_validity(
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
    fn test_limit_truncates_undefined_column() {
        let mut test_instance = DataFrame::new(vec![Column::undefined("u", 3)]);

        test_instance.limit(2).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Undefined(size) => {
                assert_eq!(*size, 2);
            }
            _ => panic!("Expected undefined column"),
        }
    }

    #[test]
    fn test_limit_handles_undefined() {
        let mut test_instance = DataFrame::new(vec![Column::undefined("u", 5)]);

        test_instance.limit(3).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Undefined(len) => assert_eq!(*len, 3),
            _ => panic!("Expected Undefined column"),
        }
    }

    #[test]
    fn test_limit_n_larger_than_len_is_safe() {
        let mut test_instance =
            DataFrame::new(vec![Column::int2_with_validity("a", [10, 20], [true, false])]);

        test_instance.limit(10).unwrap();

        assert_eq!(
            test_instance.columns[0].data,
            ColumnValues::int2_with_validity([10, 20], [true, false])
        );
    }
}
