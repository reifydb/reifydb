// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::columnar::ColumnData;
use reifydb_core::value::container::{NumberContainer, StringContainer};
use reifydb_core::result::error::diagnostic::boolean::invalid_number_boolean;
use reifydb_core::result::error::diagnostic::cast;
use reifydb_core::value::IsNumber;
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::{OwnedSpan, Type, return_error};
use std::fmt::{Debug, Display};

pub fn to_boolean(
    data: &ColumnData,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
    match data {
        ColumnData::Int1(container) => from_int1(container, &span),
        ColumnData::Int2(container) => from_int2(container, &span),
        ColumnData::Int4(container) => from_int4(container, &span),
        ColumnData::Int8(container) => from_int8(container, &span),
        ColumnData::Int16(container) => from_int16(container, &span),
        ColumnData::Uint1(container) => from_uint1(container, &span),
        ColumnData::Uint2(container) => from_uint2(container, &span),
        ColumnData::Uint4(container) => from_uint4(container, &span),
        ColumnData::Uint8(container) => from_uint8(container, &span),
        ColumnData::Uint16(container) => from_uint16(container, &span),
        ColumnData::Float4(container) => from_float4(container, &span),
        ColumnData::Float8(container) => from_float8(container, &span),
        ColumnData::Utf8(container) => from_utf8(container, span),
        _ => {
            let source_type = data.get_type();
            return_error!(cast::unsupported_cast(span(), source_type, Type::Bool))
        }
    }
}

fn to_bool<T>(
    container: &NumberContainer<T>,
    span: &impl Fn() -> OwnedSpan,
    validate: impl Fn(T) -> Option<bool>,
) -> crate::Result<ColumnData>
where
    T: Copy + Display + IsNumber + Clone + Debug + Default,
{
    let mut out = ColumnData::with_capacity(Type::Bool, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            match validate(container[idx]) {
                Some(b) => out.push::<bool>(b),
                None => {
                    let mut span = span();
                    span.fragment = container[idx].to_string();
                    return_error!(invalid_number_boolean(span));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

macro_rules! impl_integer_to_bool {
    ($fn_name:ident, $type:ty) => {
        #[inline]
        fn $fn_name(
            container: &NumberContainer<$type>,
            span: &impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnData> {
            to_bool(container, span, |val| match val {
                0 => Some(false),
                1 => Some(true),
                _ => None,
            })
        }
    };
}

macro_rules! impl_float_to_bool {
    ($fn_name:ident, $type:ty) => {
        #[inline]
        fn $fn_name(
            container: &NumberContainer<$type>,
            span: &impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnData> {
            to_bool(container, span, |val| {
                if val == 0.0 {
                    Some(false)
                } else if val == 1.0 {
                    Some(true)
                } else {
                    None
                }
            })
        }
    };
}

impl_integer_to_bool!(from_int1, i8);
impl_integer_to_bool!(from_int2, i16);
impl_integer_to_bool!(from_int4, i32);
impl_integer_to_bool!(from_int8, i64);
impl_integer_to_bool!(from_int16, i128);
impl_integer_to_bool!(from_uint1, u8);
impl_integer_to_bool!(from_uint2, u16);
impl_integer_to_bool!(from_uint4, u32);
impl_integer_to_bool!(from_uint8, u64);
impl_integer_to_bool!(from_uint16, u128);
impl_float_to_bool!(from_float4, f32);
impl_float_to_bool!(from_float8, f64);

fn from_utf8(
    container: &StringContainer,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
    let mut out = ColumnData::with_capacity(Type::Bool, container.len());
    for idx in 0..container.len() {
        if container.is_defined(idx) {
            let mut span = span();
            span.fragment = container[idx].clone();

            out.push(parse_bool(span)?);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
