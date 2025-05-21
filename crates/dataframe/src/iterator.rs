// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, DataFrame, RowRef, ValueRef};

pub struct DataFrameIter<'df> {
    pub(crate) df: &'df DataFrame,
    pub(crate) row_index: usize,
    pub(crate) row_total: usize,
    pub(crate) colum_names: Vec<String>,
}

impl<'df> Iterator for DataFrameIter<'df> {
    type Item = RowRef<'df, 'df>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_index >= self.row_total {
            return None;
        }

        let i = self.row_index;
        self.row_index += 1;

        let values = self
            .df
            .columns
            .iter()
            .map(|col| match &col.data {
                ColumnValues::Int2(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Int2(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                // ColumnValues::Float(data, bitmap) => {
                //     if bitmap[i] {
                //         ValueRef::Float(&data[i])
                //     } else {
                //         ValueRef::Undefined
                //     }
                // }
                ColumnValues::Text(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Text(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Bool(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Bool(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Undefined(_) => ValueRef::Undefined,
            })
            .collect();

        // SAFETY: we trick the borrow checker here a bit with a cast.
        // Because `self.col_names` is owned by the iterator, we need to coerce its lifetime upward.

        let col_names: &'df [String] = unsafe { std::mem::transmute(&self.colum_names[..]) };

        Some(RowRef { values, column_index: col_names, columns: &self.df.index })
    }
}
