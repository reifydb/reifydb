// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, ColumnValues, Frame, ValueRef};

pub struct TransposedRow<'a> {
    pub row_idx: usize,
    pub columns: &'a [Column],
}
impl<'a> TransposedRow<'a> {
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        self.columns.iter().map(move |col| match &col.data {
            ColumnValues::Bool(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Bool(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Float4(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Float4(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Float8(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Float8(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int1(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Int1(&v[self.row_idx])
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
            ColumnValues::Int4(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Int4(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int8(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Int8(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Int16(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Int16(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::String(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::String(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint1(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Uint1(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint2(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Uint2(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint4(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Uint4(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint8(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Uint8(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Uint16(v, valid) => {
                if valid[self.row_idx] {
                    ValueRef::Uint16(&v[self.row_idx])
                } else {
                    ValueRef::Undefined
                }
            }
            ColumnValues::Undefined(_) => ValueRef::Undefined,
        })
    }
}


impl Frame {
    pub fn transpose_view(&self) -> impl Iterator<Item = TransposedRow<'_>> {
        let row_count = self.columns.first().map_or(0, |c| c.data.len());
        (0..row_count).map(move |row_idx| TransposedRow { row_idx, columns: &self.columns })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
