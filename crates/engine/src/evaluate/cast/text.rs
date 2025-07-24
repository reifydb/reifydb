// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::frame::ColumnValues;
use reifydb_core::error::diagnostic::cast;
use reifydb_core::{BitVec, OwnedSpan, Type};
use std::fmt::Display;

pub fn to_text(values: &ColumnValues, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match values {
        ColumnValues::Bool(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Int1(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Int2(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Int4(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Int8(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Int16(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uint1(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uint2(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uint4(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uint8(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uint16(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Float4(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Float8(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Date(vals, bitvec) => from(vals, bitvec),
        ColumnValues::DateTime(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Time(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Interval(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uuid4(vals, bitvec) => from(vals, bitvec),
        ColumnValues::Uuid7(vals, bitvec) => from(vals, bitvec),
        _ => {
            let source_type = values.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, Type::Utf8))
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
