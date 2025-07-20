// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::frame::ColumnValues;
use reifydb_core::{BitVec, Type};
use std::fmt::Display;

impl ColumnValues {
    pub(crate) fn to_text(&self) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Bool(values, bitvec) => from(values, bitvec),
            ColumnValues::Int1(values, bitvec) => from(values, bitvec),
            ColumnValues::Int2(values, bitvec) => from(values, bitvec),
            ColumnValues::Int4(values, bitvec) => from(values, bitvec),
            ColumnValues::Int8(values, bitvec) => from(values, bitvec),
            ColumnValues::Int16(values, bitvec) => from(values, bitvec),
            ColumnValues::Uint1(values, bitvec) => from(values, bitvec),
            ColumnValues::Uint2(values, bitvec) => from(values, bitvec),
            ColumnValues::Uint4(values, bitvec) => from(values, bitvec),
            ColumnValues::Uint8(values, bitvec) => from(values, bitvec),
            ColumnValues::Uint16(values, bitvec) => from(values, bitvec),
            ColumnValues::Float4(values, bitvec) => from(values, bitvec),
            ColumnValues::Float8(values, bitvec) => from(values, bitvec),
            ColumnValues::Date(values, bitvec) => from(values, bitvec),
            ColumnValues::DateTime(values, bitvec) => from(values, bitvec),
            ColumnValues::Time(values, bitvec) => from(values, bitvec),
            ColumnValues::Interval(values, bitvec) => from(values, bitvec),
            _ => unreachable!(),
        }
    }
}

#[inline]
fn from<T>(values: &[T], bitvec: &BitVec) -> crate::Result<ColumnValues>
where
    T: Copy + Display,
{
    let mut out = ColumnValues::with_capacity(Type::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            out.push::<String>(val.to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
