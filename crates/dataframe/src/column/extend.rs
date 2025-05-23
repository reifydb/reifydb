// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues};

impl Column {
    pub fn extend(&mut self, other: Column) -> crate::Result<()> {
        self.data.extend(other.data)
    }
}

impl ColumnValues {
    pub fn extend(&mut self, other: ColumnValues) -> crate::Result<()> {
        match (&mut *self, other) {
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

            // Promote Undefined
            (ColumnValues::Undefined(l_len), typed_lr) => match typed_lr {
                ColumnValues::Float8(r, r_valid) => {
                    *self = ColumnValues::Float8(
                        vec![0.0f64; *l_len].into_iter().chain(r.clone()).collect(),
                        vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                    )
                }
                ColumnValues::Int2(r, r_valid) => {
                    *self = ColumnValues::Int2(
                        vec![0; *l_len].into_iter().chain(r.clone()).collect(),
                        vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                    )
                }
                ColumnValues::Text(r, r_valid) => {
                    *self = ColumnValues::Text(
                        vec![String::new(); *l_len].into_iter().chain(r.clone()).collect(),
                        vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                    )
                }
                ColumnValues::Bool(r, r_valid) => {
                    *self = ColumnValues::Bool(
                        vec![false; *l_len].into_iter().chain(r.clone()).collect(),
                        vec![false; *l_len].into_iter().chain(r_valid.clone()).collect(),
                    )
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
                return Err("column type mismatch".to_string().into());
            }
        }

        Ok(())
    }
}
