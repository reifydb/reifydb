// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, DataFrame};

impl DataFrame {
    pub fn limit(&mut self, n: usize) -> crate::Result<()> {
        let mut columns = Vec::with_capacity(self.columns.len());

        for col in &self.columns {
            let data = match &col.data {
                ColumnValues::Float8(values, valid) => ColumnValues::Float8(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Int2(values, valid) => ColumnValues::Int2(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Text(values, valid) => ColumnValues::Text(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
                ),
                ColumnValues::Bool(values, valid) => ColumnValues::Bool(
                    values[..n.min(values.len())].to_vec(),
                    valid[..n.min(valid.len())].to_vec(),
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
    fn test_truncates_int2_column() {
        let mut test_instance = DataFrame::new(vec![Column::int2_with_validity(
            "a",
            [1, 2, 3, 4],
            [true, true, false, true],
        )]);

        test_instance.limit(2).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Int2(vals, valid) => {
                assert_eq!(vals, &[1, 2]);
                assert_eq!(valid, &[true, true]);
            }
            _ => panic!("Expected Int2 column"),
        }
    }

    #[test]
    fn test_limit_truncates_text_column() {
        let mut test_instance = DataFrame::new(vec![Column::text_with_validity(
            "t",
            ["a", "b", "c"],
            [true, false, true],
        )]);

        test_instance.limit(1).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Text(vals, valid) => {
                assert_eq!(vals, &["a"]);
                assert_eq!(valid, &[true]);
            }
            _ => panic!("Expected Text column"),
        }
    }

    #[test]
    fn test_limit_truncates_bool_column() {
        let mut test_instance = DataFrame::new(vec![Column::bool_with_validity(
            "flag",
            [true, true, false],
            [false, true, true],
        )]);

        test_instance.limit(1).unwrap();

        match &test_instance.columns[0].data {
            ColumnValues::Bool(vals, valid) => {
                assert_eq!(vals, &[true]);
                assert_eq!(valid, &[false]);
            }
            _ => panic!("Expected Bool column"),
        }
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

        match &test_instance.columns[0].data {
            ColumnValues::Int2(vals, valid) => {
                assert_eq!(vals, &[10, 20]);
                assert_eq!(valid, &[true, false]);
            }
            _ => panic!("Expected Int2 column"),
        }
    }
}
