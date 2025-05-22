// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, DataFrame, ValueRef};

pub struct TransposedRow<'a> {
    pub row_idx: usize,
    pub columns: &'a [Column],
}

impl<'a> TransposedRow<'a> {
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.columns.iter().map(move |col| match &col.data {
            ColumnValues::Float8(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Float8(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int2(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Int2(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Text(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Text(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Bool(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Bool(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Undefined(_) => ValueRef::Undefined,
        })
    }
}

impl DataFrame {
    pub fn transpose_view(&self) -> impl Iterator<Item = TransposedRow<'_>> {
        let row_count = self.columns.first().map_or(0, |c| c.data.len());
        (0..row_count).map(move |row_idx| TransposedRow { row_idx, columns: &self.columns })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        todo!()
    }
}
