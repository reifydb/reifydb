// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, DataFrame};

pub trait Append<T> {
    fn append(&mut self, other: T) -> Result<(), String>;
}

impl Append<DataFrame> for DataFrame {
    fn append(&mut self, other: DataFrame) -> Result<(), String> {
        if self.columns.len() != other.columns.len() {
            return Err("Mismatched column count".to_string());
        }

        for (i, (lhs, rhs)) in self.columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
            if lhs.name != rhs.name {
                return Err(format!(
                    "Column name mismatch at index {}: '{}' vs '{}'",
                    i, lhs.name, rhs.name
                ));
            }

            match (&mut lhs.data, rhs.data) {
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
                (ColumnValues::Undefined(l_len), typed_rhs) => match typed_rhs {
                    ColumnValues::Int2(r, r_valid) => {
                        *lhs = Column {
                            name: lhs.name.clone(),
                            data: ColumnValues::Int2(
                                vec![0; *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Text(r, r_valid) => {
                        *lhs = Column {
                            name: lhs.name.clone(),
                            data: ColumnValues::Text(
                                vec![String::new(); *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Bool(r, r_valid) => {
                        *lhs = Column {
                            name: lhs.name.clone(),
                            data: ColumnValues::Bool(
                                vec![false; *l_len].into_iter().chain(r.clone()).collect(),
                                vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                            ),
                        };
                    }
                    ColumnValues::Undefined(_) => {}
                },

                // Prevent appending typed into Undefined
                (typed_lhs, ColumnValues::Undefined(r_len)) => match typed_lhs {
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
                    return Err(format!("Column type mismatch for '{}'", lhs.name));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Column, ColumnValues};

    mod dataframe {
        use crate::transform::append::Append;
        use crate::transform::append::tests::*;
        use crate::*;

        #[test]
        fn test_append_boolean() {
            let mut df1 = DataFrame::new(vec![col_bool("id", &[true], &[false])]);

            let df2 = DataFrame::new(vec![col_bool("id", &[false], &[true])]);

            df1.append(df2).unwrap();

            assert_eq!(
                df1.columns[0].data,
                ColumnValues::Bool(vec![true, false], vec![false, true])
            );
        }

        #[test]
        fn test_append_int2() {
            let mut df1 = DataFrame::new(vec![col_int2("id", &[1, 2], &[true, true])]);

            let df2 = DataFrame::new(vec![col_int2("id", &[3, 4], &[true, false])]);

            df1.append(df2).unwrap();

            assert_eq!(
                df1.columns[0].data,
                ColumnValues::Int2(vec![1, 2, 3, 4], vec![true, true, true, false])
            );
        }

        #[test]
        fn test_append_text() {
            let mut df1 = DataFrame::new(vec![col_text("id", &["a", "b"], &[true, true])]);

            let df2 = DataFrame::new(vec![col_text("id", &["c", "d"], &[true, false])]);

            df1.append(df2).unwrap();

            assert_eq!(
                df1.columns[0].data,
                ColumnValues::Text(
                    vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
                    vec![true, true, true, false]
                )
            );
        }

        #[test]
        fn test_append_with_undefined_rhs_promotes_correctly() {
            let mut df1 = DataFrame::new(vec![col_int2("id", &[1, 2], &[true, false])]);

            let df2 = DataFrame::new(vec![col_undefined("id", 2)]);

            df1.append(df2).unwrap();

            assert_eq!(
                df1.columns[0].data,
                ColumnValues::Int2(vec![1, 2, 0, 0], vec![true, false, false, false])
            );
        }

        #[test]
        fn test_append_with_undefined_lhs_promotes_correctly() {
            let mut df1 = DataFrame::new(vec![col_undefined("score", 2)]);

            let df2 = DataFrame::new(vec![col_int2("score", &[10, 20], &[true, false])]);

            df1.append(df2).unwrap();

            assert_eq!(
                df1.columns[0].data,
                ColumnValues::Int2(vec![0, 0, 10, 20], vec![false, false, true, false])
            );
        }

        #[test]
        fn test_append_fails_on_column_count_mismatch() {
            let mut df1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let df2 = DataFrame::new(vec![
                col_int2("id", &[2], &[true]),
                col_text("name", &["Bob"], &[true]),
            ]);

            let result = df1.append(df2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_column_name_mismatch() {
            let mut df1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let df2 = DataFrame::new(vec![col_int2("wrong", &[2], &[true])]);

            let result = df1.append(df2);
            assert!(result.is_err());
        }

        #[test]
        fn test_append_fails_on_type_mismatch() {
            let mut df1 = DataFrame::new(vec![col_int2("id", &[1], &[true])]);

            let df2 = DataFrame::new(vec![col_text("id", &["A"], &[true])]);

            let result = df1.append(df2);
            assert!(result.is_err());
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
