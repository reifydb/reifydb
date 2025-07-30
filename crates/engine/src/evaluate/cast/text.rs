// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::column::ColumnData;
use reifydb_core::value::container::{BoolContainer, NumberContainer, TemporalContainer, UuidContainer};
use reifydb_core::error::diagnostic::cast;
use reifydb_core::value::{IsNumber, IsTemporal, IsUuid};
use reifydb_core::{OwnedSpan, Type};
use std::fmt::{Debug, Display};

pub fn to_text(
    data: &ColumnData,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
    match data {
        ColumnData::Bool(container) => from_bool(container),
        ColumnData::Int1(container) => from_number(container),
        ColumnData::Int2(container) => from_number(container),
        ColumnData::Int4(container) => from_number(container),
        ColumnData::Int8(container) => from_number(container),
        ColumnData::Int16(container) => from_number(container),
        ColumnData::Uint1(container) => from_number(container),
        ColumnData::Uint2(container) => from_number(container),
        ColumnData::Uint4(container) => from_number(container),
        ColumnData::Uint8(container) => from_number(container),
        ColumnData::Uint16(container) => from_number(container),
        ColumnData::Float4(container) => from_number(container),
        ColumnData::Float8(container) => from_number(container),
        ColumnData::Date(container) => from_temporal(container),
        ColumnData::DateTime(container) => from_temporal(container),
        ColumnData::Time(container) => from_temporal(container),
        ColumnData::Interval(container) => from_temporal(container),
        ColumnData::Uuid4(container) => from_uuid(container),
        ColumnData::Uuid7(container) => from_uuid(container),
        _ => {
            let source_type = data.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, Type::Utf8))
        }
    }
}

#[inline]
fn from_bool(container: &BoolContainer) -> crate::Result<ColumnData> {
    let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container.data().get(idx).to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

#[inline]
fn from_number<T>(container: &NumberContainer<T>) -> crate::Result<ColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsNumber,
{
    let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
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
fn from_temporal<T>(container: &TemporalContainer<T>) -> crate::Result<ColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsTemporal,
{
    let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
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
fn from_uuid<T>(container: &UuidContainer<T>) -> crate::Result<ColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsUuid,
{
    let mut out = ColumnData::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container[idx].to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
