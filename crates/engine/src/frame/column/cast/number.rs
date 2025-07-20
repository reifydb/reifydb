// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::error;
use crate::evaluate::{Convert, Demote, Error, Promote};
use crate::frame::ColumnValues;
use crate::frame::column::cast::{convert_vec, demote_vec, number, promote_vec};
use reifydb_core::diagnostic::cast;
use reifydb_core::value::number::{parse_float, parse_int, parse_uint};
use reifydb_core::{BitVec, Span, Type};

impl ColumnValues {
    pub(crate) fn to_number(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        if self.get_type().is_number() {
            return self.number_to_number(target, ctx, span);
        }

        match target {
            Type::Int1
            | Type::Int2
            | Type::Int4
            | Type::Int8
            | Type::Int16
            | Type::Uint1
            | Type::Uint2
            | Type::Uint4
            | Type::Uint8
            | Type::Uint16 => {
                if self.is_bool() {
                    return self.bool_to_numeric_vec(target);
                }

                if self.is_utf8() {
                    return self.text_to_numeric_vec(target, span);
                }

                if self.is_float() {
                    return self.float_to_number(target);
                }

                unreachable!()
            }
            Type::Float4 | Type::Float8 => {
                match self {
                    ColumnValues::Bool(values, bitvec) => {
                        // return number::bool_to_numeric_vec(values, bitvec, target);
                        return self.bool_to_numeric_vec(target);
                    }
                    ColumnValues::Utf8(values, bitvec) => {
                        return number::text_to_float_vec(values, bitvec, target, span);
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl ColumnValues {
    pub(crate) fn bool_to_numeric_vec(&self, target: Type) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Bool(values, bitvec) => {
                let mut out = ColumnValues::with_capacity(target, values.len());
                for (idx, &val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        match target {
                            Type::Int1 => out.push::<i8>(if val { 1i8 } else { 0i8 }),
                            Type::Int2 => out.push::<i16>(if val { 1i16 } else { 0i16 }),
                            Type::Int4 => out.push::<i32>(if val { 1i32 } else { 0i32 }),
                            Type::Int8 => out.push::<i64>(if val { 1i64 } else { 0i64 }),
                            Type::Int16 => out.push::<i128>(if val { 1i128 } else { 0i128 }),
                            Type::Uint1 => out.push::<u8>(if val { 1u8 } else { 0u8 }),
                            Type::Uint2 => out.push::<u16>(if val { 1u16 } else { 0u16 }),
                            Type::Uint4 => out.push::<u32>(if val { 1u32 } else { 0u32 }),
                            Type::Uint8 => out.push::<u64>(if val { 1u64 } else { 0u64 }),
                            Type::Uint16 => out.push::<u128>(if val { 1u128 } else { 0u128 }),
                            Type::Float4 => out.push::<f32>(if val { 1.0f32 } else { 0.0f32 }),
                            Type::Float8 => out.push::<f64>(if val { 1.0f64 } else { 0.0f64 }),
                            _ => unreachable!(),
                        }
                    } else {
                        out.push_undefined();
                    }
                }
                Ok(out)
            }
            _ => unreachable!(),
        }
    }
}

impl ColumnValues {
    pub(crate) fn float_to_number(&self, target: Type) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Float4(values, bitvec) => match target {
                Type::Int1 => f32_to_i8_vec(values, bitvec),
                Type::Int2 => f32_to_i16_vec(values, bitvec),
                Type::Int4 => f32_to_i32_vec(values, bitvec),
                Type::Int8 => f32_to_i64_vec(values, bitvec),
                Type::Int16 => f32_to_i128_vec(values, bitvec),
                Type::Uint1 => f32_to_u8_vec(values, bitvec),
                Type::Uint2 => f32_to_u16_vec(values, bitvec),
                Type::Uint4 => f32_to_u32_vec(values, bitvec),
                Type::Uint8 => f32_to_u64_vec(values, bitvec),
                Type::Uint16 => f32_to_u128_vec(values, bitvec),
                _ => unreachable!(),
            },
            ColumnValues::Float8(values, bitvec) => match target {
                Type::Int1 => f64_to_i8_vec(values, bitvec),
                Type::Int2 => f64_to_i16_vec(values, bitvec),
                Type::Int4 => f64_to_i32_vec(values, bitvec),
                Type::Int8 => f64_to_i64_vec(values, bitvec),
                Type::Int16 => f64_to_i128_vec(values, bitvec),
                Type::Uint1 => f64_to_u8_vec(values, bitvec),
                Type::Uint2 => f64_to_u16_vec(values, bitvec),
                Type::Uint4 => f64_to_u32_vec(values, bitvec),
                Type::Uint8 => f64_to_u64_vec(values, bitvec),
                Type::Uint16 => f64_to_u128_vec(values, bitvec),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
}

impl ColumnValues {
    pub(crate) fn text_to_numeric_vec(
        &self,
        target: Type,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Utf8(values, bitvec) => {
                let mut out = ColumnValues::with_capacity(target, values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        // Create a temporary span for parsing
                        let temp_span = Span {
                            fragment: val.clone(),
                            line: span().line,
                            column: span().column,
                        };

                        // Try to parse based on the target type
                        match target {
                            Type::Int1 => {
                                out.push::<i8>(parse_int::<i8>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Int1, e.diagnostic()))
                                })?)
                            }
                            Type::Int2 => {
                                out.push::<i16>(parse_int::<i16>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Int2, e.diagnostic()))
                                })?)
                            }
                            Type::Int4 => {
                                out.push::<i32>(parse_int::<i32>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Int4, e.diagnostic()))
                                })?)
                            }
                            Type::Int8 => {
                                out.push::<i64>(parse_int::<i64>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Int8, e.diagnostic()))
                                })?)
                            }
                            Type::Int16 => {
                                out.push::<i128>(parse_int::<i128>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Int16, e.diagnostic()))
                                })?)
                            }
                            Type::Uint1 => {
                                out.push::<u8>(parse_uint::<u8>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Uint1, e.diagnostic()))
                                })?)
                            }
                            Type::Uint2 => {
                                out.push::<u16>(parse_uint::<u16>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Uint2, e.diagnostic()))
                                })?)
                            }
                            Type::Uint4 => {
                                out.push::<u32>(parse_uint::<u32>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Uint4, e.diagnostic()))
                                })?)
                            }
                            Type::Uint8 => {
                                out.push::<u64>(parse_uint::<u64>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(span(), Type::Uint8, e.diagnostic()))
                                })?)
                            }
                            Type::Uint16 => {
                                out.push::<u128>(parse_uint::<u128>(&temp_span).map_err(|e| {
                                    Error(cast::invalid_number(
                                        span(),
                                        Type::Uint16,
                                        e.diagnostic(),
                                    ))
                                })?)
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        out.push_undefined();
                    }
                }
                return Ok(out);
            }
            _ => unreachable!(),
        }
    }
}

// Cast from text to float
pub fn text_to_float_vec(
    values: &[String],
    bitvec: &BitVec,
    target: Type,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(target, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            match target {
                Type::Float4 => out.push::<f32>(parse_float::<f32>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Float4, e.diagnostic()))
                })?),

                Type::Float8 => out.push::<f64>(parse_float::<f64>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Float8, e.diagnostic()))
                })?),
                _ => unreachable!(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

// Float32 to integer conversion functions
pub fn f32_to_i8_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i8::MIN as f32 && truncated <= i8::MAX as f32 {
                out.push::<i8>(truncated as i8);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_i16_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i16::MIN as f32 && truncated <= i16::MAX as f32 {
                out.push::<i16>(truncated as i16);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_i32_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i32::MIN as f32 && truncated <= i32::MAX as f32 {
                out.push::<i32>(truncated as i32);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_i64_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i64::MIN as f32 && truncated <= i64::MAX as f32 {
                out.push::<i64>(truncated as i64);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_i128_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i128::MIN as f32 && truncated <= i128::MAX as f32 {
                out.push::<i128>(truncated as i128);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_u8_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u8::MAX as f32 {
                out.push::<u8>(truncated as u8);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_u16_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u16::MAX as f32 {
                out.push::<u16>(truncated as u16);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_u32_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u32::MAX as f32 {
                out.push::<u32>(truncated as u32);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_u64_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u64::MAX as f32 {
                out.push::<u64>(truncated as u64);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f32_to_u128_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u128::MAX as f32 {
                out.push::<u128>(truncated as u128);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

// Float64 to integer conversion functions
pub fn f64_to_i8_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                out.push::<i8>(truncated as i8);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_i16_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                out.push::<i16>(truncated as i16);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_i32_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                out.push::<i32>(truncated as i32);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_i64_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                out.push::<i64>(truncated as i64);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_i128_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Int16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                out.push::<i128>(truncated as i128);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_u8_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                out.push::<u8>(truncated as u8);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_u16_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                out.push::<u16>(truncated as u16);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_u32_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                out.push::<u32>(truncated as u32);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_u64_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                out.push::<u64>(truncated as u64);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub fn f64_to_u128_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Uint16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let truncated = val.trunc();
            if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                out.push::<u128>(truncated as u128);
            } else {
                out.push_undefined();
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

impl ColumnValues {
    pub fn number_to_number(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        if !target.is_number() {
            return Err(error::Error::Evaluation(Error(
                reifydb_core::diagnostic::cast::unsupported_cast(span(), self.get_type(), target),
            )));
        }

        macro_rules! cast {
            (
                $src_variant:ident, $src_ty:ty,
                promote => [ $( ($pro_variant:ident, $pro_ty:ty) ),* ],
                demote => [ $( ($dem_variant:ident, $dem_ty:ty) ),* ],
                convert => [ $( ($con_variant:ident, $con_ty:ty) ),* ]
            ) => {
                if let ColumnValues::$src_variant(values, bitvec) = self {
                    match target {
                        $(
                            Type::$pro_variant => return promote_vec::<$src_ty, $pro_ty>(
                                values,
                                bitvec,
                                ctx,
                                &span,
                                Type::$pro_variant,
                                ColumnValues::push::<$pro_ty>,
                            ),
                        )*
                        $(
                            Type::$dem_variant => return demote_vec::<$src_ty, $dem_ty>(
                                    values,
                                    bitvec,
                                    ctx,
                                    &span,
                                    Type::$dem_variant,
                                    ColumnValues::push::<$dem_ty>,
                                ),
                        )*
                        $(
                            Type::$con_variant => return convert_vec::<$src_ty, $con_ty>(
                                values,
                                bitvec,
                                ctx,
                                &span,
                                Type::$con_variant,
                                ColumnValues::push::<$con_ty>,
                            ),
                        )*
                        _ => {}
                    }
                }
            }
        }

        cast!(Float4, f32,
            promote => [(Float8, f64) ],
            demote => [ ],
            convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Float8, f64,
            promote => [ ],
            demote => [(Float4, f32)],
            convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Int1, i8,
            promote => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
            demote => [],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Int2, i16,
            promote => [(Int4, i32), (Int8, i64), (Int16, i128)],
            demote => [(Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Int4, i32,
            promote => [(Int8, i64), (Int16, i128)],
            demote => [(Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Int8, i64,
            promote => [(Int16, i128)],
            demote => [(Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Int16, i128,
            promote => [],
            demote => [(Int8, i64), (Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        cast!(Uint1, u8,
            promote => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        cast!(Uint2, u16,
            promote => [(Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [(Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        cast!(Uint4, u32,
            promote => [(Uint8, u64), (Uint16, u128)],
            demote => [(Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        cast!(Uint8, u64,
            promote => [(Uint16, u128)],
            demote => [(Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        cast!(Uint16, u128,
            promote => [],
            demote => [(Uint8, u64), (Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        unreachable!(
            "number_to_number: unhandled conversion from {:?} to {:?}",
            self.get_type(),
            target
        )
    }
}
