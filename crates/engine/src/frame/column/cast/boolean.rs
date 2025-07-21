// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::frame::ColumnValues;
use reifydb_core::error::diagnostic::boolean::invalid_number_boolean;
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::{BitVec, OwnedSpan, Type};

impl ColumnValues {
    pub fn to_boolean(&self, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Int1(values, bitvec) => from_int1(values, bitvec, &span),
            ColumnValues::Int2(values, bitvec) => from_int2(values, bitvec, &span),
            ColumnValues::Int4(values, bitvec) => from_int4(values, bitvec, &span),
            ColumnValues::Int8(values, bitvec) => from_int8(values, bitvec, &span),
            ColumnValues::Int16(values, bitvec) => from_int16(values, bitvec, &span),
            ColumnValues::Uint1(values, bitvec) => from_uint1(values, bitvec, &span),
            ColumnValues::Uint2(values, bitvec) => from_uint2(values, bitvec, &span),
            ColumnValues::Uint4(values, bitvec) => from_uint4(values, bitvec, &span),
            ColumnValues::Uint8(values, bitvec) => from_uint8(values, bitvec, &span),
            ColumnValues::Uint16(values, bitvec) => from_uint16(values, bitvec, &span),
            ColumnValues::Float4(values, bitvec) => from_float4(values, bitvec, &span),
            ColumnValues::Float8(values, bitvec) => from_float8(values, bitvec, &span),
            ColumnValues::Utf8(values, bitvec) => from_utf8(values, bitvec, span),
            _ => unreachable!(),
        }
    }
}

fn to_bool<T>(
    values: &[T],
    bitvec: &BitVec,
    span: &impl Fn() -> OwnedSpan,
    validate: impl Fn(T) -> Option<bool>,
) -> crate::Result<ColumnValues>
where
    T: Copy + std::fmt::Display,
{
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match validate(val) {
                Some(b) => out.push::<bool>(b),
                None => {
                    let mut span = span();
                    span.fragment = val.to_string();
                    return Err(reifydb_core::Error(invalid_number_boolean(span)));
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
            values: &[$type],
            bitvec: &BitVec,
            span: &impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnValues> {
            to_bool(values, bitvec, span, |val| match val {
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
            values: &[$type],
            bitvec: &BitVec,
            span: &impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnValues> {
            to_bool(values, bitvec, span, |val| {
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
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let mut span = span();
            span.fragment = val.clone();

            out.push(parse_bool(span)?);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}
