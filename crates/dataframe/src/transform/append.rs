// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, DataFrame};
use base::{ColumnValues, Row, Value};

pub trait Append<T> {
    fn append(&mut self, other: T) -> crate::Result<()>;
}

impl Append<DataFrame> for DataFrame {
    fn append(&mut self, other: DataFrame) -> crate::Result<()> {
        if self.columns.len() != other.columns.len() {
            return Err("mismatched column count".into());
        }

        for (i, (l, lr)) in self.columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
            if l.name != lr.name {
                return Err(format!(
                    "column name mismatch at index {}: '{}' vs '{}'",
                    i, l.name, lr.name
                )
                .into());
            }

            match (&mut l.data, lr.data) {
                (ColumnValues::Float8(l, l_valid), ColumnValues::Float8(r, r_valid)) => {
                    l.extend(r);
                    l_valid.extend(r_valid);
                }

                (ColumnValues::Int2(l, l_valid), ColumnValues::Int2(r, r_valid)) => {
                    l.extend(r);
                    l_valid.extend(r_valid);
                }

                (ColumnValues::Text(l, l_valid), ColumnValues::Text(r, r_valid)) => {
                    l.extend(r);
                    l_valid.extend(r_valid);
                }

                (ColumnValues::Bool(l, l_valid), ColumnValues::Bool(r, r_valid)) => {
                    l.extend(r);
                    l_valid.extend(r_valid);
                }

                (ColumnValues::Undefined(l_len), ColumnValues::Undefined(r_len)) => {
                    *l_len += r_len;
                }

                // Promote Undefined → typed if needed
                (ColumnValues::Undefined(l_len), typed_lr) => match typed_lr {
                    ColumnValues::Float8(r, r_valid) => {
                        *l = Column {
                            name: l.name.clone(),
                            data: ColumnValues::Float8(
                                vec![0.0f64; *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Int2(r, r_valid) => {
                        *l = Column {
                            name: l.name.clone(),
                            data: ColumnValues::Int2(
                                vec![0; *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Text(r, r_valid) => {
                        *l = Column {
                            name: l.name.clone(),
                            data: ColumnValues::Text(
                                vec![String::new(); *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Bool(r, r_valid) => {
                        *l = Column {
                            name: l.name.clone(),
                            data: ColumnValues::Bool(
                                vec![false; *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Undefined(_) => {}
                },

                // Prevent appending typed into Undefined
                (typed_l, ColumnValues::Undefined(r_len)) => match typed_l {
                    ColumnValues::Float8(l, l_valid) => {
                        l.extend(std::iter::repeat(0.0f64).take(r_len));
                        l_valid.extend(std::iter::repeat(false).take(r_len));
                    }
                    ColumnValues::Int2(l, l_valid) => {
                        l.extend(std::iter::repeat(0).take(r_len));
                        l_valid.extend(std::iter::repeat(false).take(r_len));
                    }
                    ColumnValues::Text(l, l_valid) => {
                        l.extend(std::iter::repeat(String::new()).take(r_len));
                        l_valid.extend(std::iter::repeat(false).take(r_len));
                    }
                    ColumnValues::Bool(l, l_valid) => {
                        l.extend(std::iter::repeat(false).take(r_len));
                        l_valid.extend(std::iter::repeat(false).take(r_len));
                    }
                    ColumnValues::Undefined(_) => unreachable!(),
                },

                (_, _) => {
                    return Err(format!("column type mismatch for '{}'", l.name).into());
                }
            }
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
    use crate::Column;
    use base::ColumnValues;

    mod dataframe {
        use crate::transform::append::Append;
        use crate::transform::append::tests::*;
        use crate::*;
        use base::ColumnValues;

        #[test]
        fn test_append_boolean() {
            let mut test_instance1 = DataFrame::new(vec![col_bool("id", &[true], &[false])]);

            let test_instance2 = DataFrame::new(vec![col_bool("id", &[false], &[true])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0].data,
                ColumnValues::Bool(vec![true, false], vec![false, true])
            );
        }

        #[test]
        fn test_append_int2() {
            let mut test_instance1 = DataFrame::new(vec![col_int2("id", &[1, 2], &[true, true])]);

            let test_instance2 = DataFrame::new(vec![col_int2("id", &[3, 4], &[true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0].data,
                ColumnValues::Int2(vec![1, 2, 3, 4], vec![true, true, true, false])
            );
        }

        #[test]
        fn test_append_text() {
            let mut test_instance1 =
                DataFrame::new(vec![col_text("id", &["a", "b"], &[true, true])]);

            let test_instance2 = DataFrame::new(vec![col_text("id", &["c", "d"], &[true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0].data,
                ColumnValues::Text(
                    vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
                    vec![true, true, true, false]
                )
            );
        }

        #[test]
        fn test_append_with_undefined_lr_promotes_correctly() {
            let mut test_instance1 = DataFrame::new(vec![col_int2("id", &[1, 2], &[true, false])]);

            let test_instance2 = DataFrame::new(vec![col_undefined("id", 2)]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0].data,
                ColumnValues::Int2(vec![1, 2, 0, 0], vec![true, false, false, false])
            );
        }

        #[test]
        fn test_append_with_undefined_l_promotes_correctly() {
            let mut test_instance1 = DataFrame::new(vec![col_undefined("score", 2)]);

            let test_instance2 = DataFrame::new(vec![col_int2("score", &[10, 20], &[true, false])]);

            test_instance1.append(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0].data,
                ColumnValues::Int2(vec![0, 0, 10, 20], vec![false, false, true, false])
            );
        }

        #[test]
        fn test_append_fails_on_column_count_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let test_instance2 = DataFrame::new(vec![
                col_int2("id", &[2], &[true]),
                col_text("name", &["Bob"], &[true]),
            ]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_column_name_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let test_instance2 = DataFrame::new(vec![col_int2("wrong", &[2], &[true])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_type_mismatch() {
            let mut test_instance1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let test_instance2 = DataFrame::new(vec![col_text("id", &["A"], &[true])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }
    }

    mod row {
        use crate::{Append, Column, DataFrame};
        use base::{ColumnValues, Value};

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

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::Int2(vec![1, 2], vec![true, true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::Text(vec!["Alice".into(), "Bob".into()], vec![true, true])
            );
            assert_eq!(
                test_instance.columns[2].data,
                ColumnValues::Bool(vec![true, false], vec![true, true])
            );
        }

        #[test]
        fn test_append_row_with_nulls() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![Value::Undefined, Value::Text("Karen".into()), Value::Undefined];

            test_instance.append(row).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::Int2(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::Text(vec!["Alice".into(), "Karen".into()], vec![true, true])
            );
            assert_eq!(
                test_instance.columns[2].data,
                ColumnValues::Bool(vec![true, false], vec![true, false])
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

    fn col_int2(name: &str, vals: &[i16], valid: &[bool]) -> Column {
        Column { name: name.to_string(), data: ColumnValues::Int2(vals.to_vec(), valid.to_vec()) }
    }

    fn col_text(name: &str, vals: &[&str], valid: &[bool]) -> Column {
        Column {
            name: name.to_string(),
            data: ColumnValues::Text(vals.iter().map(|s| s.to_string()).collect(), valid.to_vec()),
        }
    }

    fn col_bool(name: &str, vals: &[bool], valid: &[bool]) -> Column {
        Column { name: name.to_string(), data: ColumnValues::Bool(vals.to_vec(), valid.to_vec()) }
    }

    fn col_undefined(name: &str, len: usize) -> Column {
        Column { name: name.to_string(), data: ColumnValues::Undefined(len) }
    }
}
