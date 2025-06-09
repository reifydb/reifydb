// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::ColumnValues;

impl ColumnValues {
    pub fn validity(&self) -> &[bool] {
        match self {
            ColumnValues::Bool(_, validity) => validity.as_slice(),
            ColumnValues::Float4(_, validity) => validity.as_slice(),
            ColumnValues::Float8(_, validity) => validity.as_slice(),
            ColumnValues::Int1(_, validity) => validity.as_slice(),
            ColumnValues::Int2(_, validity) => validity.as_slice(),
            ColumnValues::Int4(_, validity) => validity.as_slice(),
            ColumnValues::Int8(_, validity) => validity.as_slice(),
            ColumnValues::Int16(_, validity) => validity.as_slice(),
            ColumnValues::String(_, validity) => validity.as_slice(),
            ColumnValues::Uint1(_, validity) => validity.as_slice(),
            ColumnValues::Uint2(_, validity) => validity.as_slice(),
            ColumnValues::Uint4(_, validity) => validity.as_slice(),
            ColumnValues::Uint8(_, validity) => validity.as_slice(),
            ColumnValues::Uint16(_, validity) => validity.as_slice(),
            ColumnValues::Undefined(_) => unreachable!("undefined has no validity mask"),
        }
    }
}
