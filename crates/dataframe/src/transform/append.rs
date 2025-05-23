// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, DataFrame};
use base::{Row, Value};

pub trait Append<T> {
    fn append(&mut self, other: T) -> crate::Result<()>;
}

impl Append<DataFrame> for DataFrame {
    fn append(&mut self, other: DataFrame) -> crate::Result<()> {
        if self.columns.len() != other.columns.len() {
            return Err("mismatched column count".into());
        }

        for (i, (l, r)) in self.columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
            if l.name != r.name {
                return Err(format!(
                    "column name mismatch at index {}: '{}' vs '{}'",
                    i, l.name, r.name
                )
                .into());
            }

            l.merge(r)?;
        }

        Ok(())
    }
}

impl Append<Row> for DataFrame {
    fn append(&mut self, other: Row) -> crate::Result<()> {
        if self.columns.len() != other.len() {
            return Err(format!(
                "mismatched column count: expected {}, got {}",
                self.columns.len(),
                other.len()
            )
            .into());
        }

        for (col, value) in self.columns.iter_mut().zip(other.into_iter()) {
            match (&mut col.data, value) {
                (ColumnValues::Int2(vec, valid), Value::Int2(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int2(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Text(vec, valid), Value::Text(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Text(vec, valid), Value::Undefined) => {
                    vec.push(String::new());
                    valid.push(false);
                }

                (ColumnValues::Bool(vec, valid), Value::Bool(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Bool(vec, valid), Value::Undefined) => {
                    vec.push(false);
                    valid.push(false);
                }

                (ColumnValues::Undefined(n), Value::Undefined) => {
                    *n += 1;
                }

                (ColumnValues::Undefined(n), v) => {
                    let mut new_column = match v {
                        Value::Int2(i) => ColumnValues::Int2(
                            vec![0; *n].into_iter().chain([i]).collect(),
                            vec![false; *n].into_iter().chain([true]).collect(),
                        ),
                        Value::Text(s) => ColumnValues::Text(
                            vec![String::new(); *n].into_iter().chain([s]).collect(),
                            vec![false; *n].into_iter().chain([true]).collect(),
                        ),
                        Value::Bool(b) => ColumnValues::Bool(
                            vec![false; *n].into_iter().chain([b]).collect(),
                            vec![false; *n].into_iter().chain([true]).collect(),
                        ),
                        Value::Float4(_) => unimplemented!(),
                        Value::Float8(_) => unimplemented!(),
                        Value::Uint2(_) => unimplemented!(),
                        Value::Undefined => unreachable!(), // already matched above
                    };

                    std::mem::swap(&mut col.data, &mut new_column);
                }

                (_, v) => {
                    return Err(format!(
                        "type mismatch for column '{}': incompatible with value {:?}",
                        col.name, v
                    )
                    .into());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    mod dataframe {
        use crate::transform::append::Append;
        use crate::*;

        #[test]
        fn test_append_boolean() {
            let mut test_instance1 =
                DataFrame::new(vec![Column::bool_with_validity("id", [true], [false])]);

            let test_instance2 =
                DataFrame::new(vec![Column::bool_with_validity("id", [false], [true])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::bool_with_validity("id", [true, false], [false, true])
            );
        }

        #[test]
        fn test_append_int2() {
            let mut test_instance1 = DataFrame::new(vec![Column::int2("id", [1, 2])]);

            let test_instance2 =
                DataFrame::new(vec![Column::int2_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_append_text() {
            let mut test_instance1 =
                DataFrame::new(vec![Column::text_with_validity("id", ["a", "b"], [true, true])]);

            let test_instance2 =
                DataFrame::new(vec![Column::text_with_validity("id", ["c", "d"], [true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::text_with_validity("id", ["a", "b", "c", "d"], [true, true, true, false])
            );
        }

        #[test]
        fn test_append_with_undefined_lr_promotes_correctly() {
            let mut test_instance1 =
                DataFrame::new(vec![Column::int2_with_validity("id", [1, 2], [true, false])]);

            let test_instance2 = DataFrame::new(vec![Column::undefined("id", 2)]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("id", [1, 2, 0, 0], [true, false, false, false])
            );
        }

        #[test]
        fn test_append_with_undefined_l_promotes_correctly() {
            let mut test_instance1 = DataFrame::new(vec![Column::undefined("score", 2)]);

            let test_instance2 =
                DataFrame::new(vec![Column::int2_with_validity("score", [10, 20], [true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("score", [0, 0, 10, 20], [false, false, true, false])
            );
        }

        #[test]
        fn test_append_fails_on_column_count_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![Column::int2("id", [1])]);

            let test_instance2 =
                DataFrame::new(vec![Column::int2("id", [2]), Column::text("name", ["Bob"])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_column_name_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = DataFrame::new(vec![Column::int2("wrong", [2])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_type_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = DataFrame::new(vec![Column::text("id", ["A"])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }
    }

    mod row {
        use crate::{Append, Column, ColumnValues, DataFrame};
        use base::Value;

        #[test]
        fn test_append_to_empty() {
            let mut test_instance = DataFrame::new(vec![]);

            let row = vec![Value::Int2(2), Value::Text("Bob".into()), Value::Bool(false)];

            let err = test_instance.append(row).err().unwrap();
            assert_eq!(err.to_string(), "mismatched column count: expected 0, got 3");
        }

        #[test]
        fn test_append_row_matching_types() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![Value::Int2(2), Value::Text("Bob".into()), Value::Bool(false)];

            test_instance.append(row).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([1, 2]));
            assert_eq!(test_instance.columns[1].data, ColumnValues::text(["Alice", "Bob"]));
            assert_eq!(test_instance.columns[2].data, ColumnValues::bool([true, false]));
        }

        #[test]
        fn test_append_row_with_undefined() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![Value::Undefined, Value::Text("Karen".into()), Value::Undefined];

            test_instance.append(row).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::text_with_validity(["Alice", "Karen"], [true, true])
            );
            assert_eq!(
                test_instance.columns[2].data,
                ColumnValues::bool_with_validity([true, false], [true, false])
            );
        }

        #[test]
        fn test_append_row_with_type_mismatch_fails() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![
                Value::Bool(true), // should be Int2
                Value::Text("Eve".into()),
                Value::Bool(false),
            ];

            let result = test_instance.append(row);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("type mismatch"));
        }

        #[test]
        fn test_append_row_wrong_length_fails() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![
                Value::Int2(42),
                Value::Text("X".into()),
                Value::Bool(true),
                Value::Bool(false),
            ];

            let result = test_instance.append(row);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("mismatched column count"));
        }

        #[test]
        fn test_append_row_to_undefined_columns_promotes() {
            let mut test_instance = DataFrame::new(vec![
                Column { name: "age".into(), data: ColumnValues::Undefined(1) },
                Column { name: "name".into(), data: ColumnValues::Undefined(1) },
            ]);

            let row = vec![Value::Int2(30), Value::Text("Zoe".into())];
            test_instance.append(row).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::Int2(vec![0, 30], vec![false, true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::Text(vec!["".into(), "Zoe".into()], vec![false, true])
            );
        }

        fn test_instance_with_columns() -> DataFrame {
            DataFrame::new(vec![
                Column { name: "int2".into(), data: ColumnValues::Int2(vec![1], vec![true]) },
                Column {
                    name: "text".into(),
                    data: ColumnValues::Text(vec!["Alice".into()], vec![true]),
                },
                Column { name: "bool".into(), data: ColumnValues::Bool(vec![true], vec![true]) },
            ])
        }
    }
}
