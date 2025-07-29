// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::error::diagnostic::cast;
use reifydb_core::frame::ColumnValues;
use reifydb_core::frame::column::container::{
    BoolContainer, NumberContainer, TemporalContainer, UuidContainer,
};
use reifydb_core::value::{IsNumber, IsTemporal, IsUuid};
use reifydb_core::{OwnedSpan, Type};
use std::fmt::{Debug, Display};

pub fn to_text(values: &ColumnValues, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match values {
        ColumnValues::Bool(container) => from_bool(container),
        ColumnValues::Int1(container) => from_number(container),
        ColumnValues::Int2(container) => from_number(container),
        ColumnValues::Int4(container) => from_number(container),
        ColumnValues::Int8(container) => from_number(container),
        ColumnValues::Int16(container) => from_number(container),
        ColumnValues::Uint1(container) => from_number(container),
        ColumnValues::Uint2(container) => from_number(container),
        ColumnValues::Uint4(container) => from_number(container),
        ColumnValues::Uint8(container) => from_number(container),
        ColumnValues::Uint16(container) => from_number(container),
        ColumnValues::Float4(container) => from_number(container),
        ColumnValues::Float8(container) => from_number(container),
        ColumnValues::Date(container) => from_temporal(container),
        ColumnValues::DateTime(container) => from_temporal(container),
        ColumnValues::Time(container) => from_temporal(container),
        ColumnValues::Interval(container) => from_temporal(container),
        ColumnValues::Uuid4(container) => from_uuid(container),
        ColumnValues::Uuid7(container) => from_uuid(container),
        _ => {
            let source_type = values.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, Type::Utf8))
        }
    }
}

#[inline]
fn from_bool(container: &BoolContainer) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container.values().get(idx).to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

#[inline]
fn from_number<T>(container: &NumberContainer<T>) -> crate::Result<ColumnValues>
where
    T: Copy + Display + Clone + Debug + Default + IsNumber,
{
    let mut out = ColumnValues::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container[idx].to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

#[inline]
fn from_temporal<T>(container: &TemporalContainer<T>) -> crate::Result<ColumnValues>
where
    T: Copy + Display + Clone + Debug + Default + IsTemporal,
{
    let mut out = ColumnValues::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container[idx].to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

#[inline]
fn from_uuid<T>(container: &UuidContainer<T>) -> crate::Result<ColumnValues>
where
    T: Copy + Display + Clone + Debug + Default + IsUuid,
{
    let mut out = ColumnValues::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container[idx].to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
