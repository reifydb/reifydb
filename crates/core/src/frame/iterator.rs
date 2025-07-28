// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame, RowRef, ValueRef};
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
            .map(|col| match &col.values() {
                ColumnValues::Bool(data, bitmap) => {
                    if bitmap.get(i) {
                        // TODO: This is a temporary workaround - we need to restructure ValueRef
                        // to handle owned values for BitVec since we can't get references to bits
                        static TRUE_VAL: bool = true;
                        static FALSE_VAL: bool = false;
                        if data.get(i) {
                            ValueRef::Bool(&TRUE_VAL)
                        } else {
                            ValueRef::Bool(&FALSE_VAL)
                        }
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Float4(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Float4(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Float8(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Float8(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int1(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Int1(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int2(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Int2(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int4(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Int4(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int8(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Int8(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Int16(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Int16(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uint1(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Uint1(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uint2(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Uint2(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uint4(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Uint4(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uint8(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Uint8(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uint16(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Uint16(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Utf8(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::String(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Date(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Date(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::DateTime(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::DateTime(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Time(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Time(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Interval(data, bitmap) => {
                    if bitmap.get(i) {
                        ValueRef::Interval(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::RowId(data, bitvec) => {
                    if bitvec.get(i) {
                        ValueRef::RowId(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uuid4(data, bitvec) => {
                    if bitvec.get(i) {
                        ValueRef::Uuid4(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Uuid7(data, bitvec) => {
                    if bitvec.get(i) {
                        ValueRef::Uuid7(&data[i])
                    } else {
                        ValueRef::Undefined
                    }
                }
                ColumnValues::Blob(data, bitvec) => {
                    if bitvec.get(i) {
                        ValueRef::Blob(&data[i])
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
