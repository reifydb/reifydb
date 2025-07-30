// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::column::EngineColumnData;
use reifydb_core::value::container::{BoolContainer, NumberContainer, TemporalContainer, UuidContainer};
use reifydb_core::error::diagnostic::cast;
use reifydb_core::value::{IsNumber, IsTemporal, IsUuid};
use reifydb_core::{OwnedSpan, Type};
use std::fmt::{Debug, Display};

pub fn to_text(
    data: &EngineColumnData,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<EngineColumnData> {
    match data {
        EngineColumnData::Bool(container) => from_bool(container),
        EngineColumnData::Int1(container) => from_number(container),
        EngineColumnData::Int2(container) => from_number(container),
        EngineColumnData::Int4(container) => from_number(container),
        EngineColumnData::Int8(container) => from_number(container),
        EngineColumnData::Int16(container) => from_number(container),
        EngineColumnData::Uint1(container) => from_number(container),
        EngineColumnData::Uint2(container) => from_number(container),
        EngineColumnData::Uint4(container) => from_number(container),
        EngineColumnData::Uint8(container) => from_number(container),
        EngineColumnData::Uint16(container) => from_number(container),
        EngineColumnData::Float4(container) => from_number(container),
        EngineColumnData::Float8(container) => from_number(container),
        EngineColumnData::Date(container) => from_temporal(container),
        EngineColumnData::DateTime(container) => from_temporal(container),
        EngineColumnData::Time(container) => from_temporal(container),
        EngineColumnData::Interval(container) => from_temporal(container),
        EngineColumnData::Uuid4(container) => from_uuid(container),
        EngineColumnData::Uuid7(container) => from_uuid(container),
        _ => {
            let source_type = data.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, Type::Utf8))
        }
    }
}

#[inline]
fn from_bool(container: &BoolContainer) -> crate::Result<EngineColumnData> {
    let mut out = EngineColumnData::with_capacity(Type::Utf8, container.len());
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
fn from_number<T>(container: &NumberContainer<T>) -> crate::Result<EngineColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsNumber,
{
    let mut out = EngineColumnData::with_capacity(Type::Utf8, container.len());
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
fn from_temporal<T>(container: &TemporalContainer<T>) -> crate::Result<EngineColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsTemporal,
{
    let mut out = EngineColumnData::with_capacity(Type::Utf8, container.len());
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
fn from_uuid<T>(container: &UuidContainer<T>) -> crate::Result<EngineColumnData>
where
    T: Copy + Display + Clone + Debug + Default + IsUuid,
{
    let mut out = EngineColumnData::with_capacity(Type::Utf8, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            out.push::<String>(container[idx].to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
