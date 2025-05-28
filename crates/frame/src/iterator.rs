// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, Frame, RowRef, ValueRef};
use std::sync::Arc;

pub struct FrameIter<'df> {
    pub(crate) df: &'df Frame,
    pub(crate) row_index: usize,
    pub(crate) row_total: usize,
    pub(crate) column_index: Arc<Vec<String>>,
}

impl<'df> Iterator for FrameIter<'df> {
    type Item = RowRef<'df>;

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
                ColumnValues::Float8(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Float8(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int2(data, bitmap) => {
                    if bitmap[i] {
                        ValueRef::Int2(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
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

        Some(RowRef { values, column_index: self.column_index.clone(), columns: &self.df.index })
    }
}
