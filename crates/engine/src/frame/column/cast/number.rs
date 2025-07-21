// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::evaluate::{Convert, Demote, Promote};
use crate::frame::ColumnValues;
use reifydb_core::error::diagnostic::cast;
use reifydb_core::value::number::{
    SafeConvert, SafeDemote, SafePromote, parse_float, parse_int, parse_uint,
};
use reifydb_core::{BitVec, GetType, OwnedSpan, Span, Type};

impl ColumnValues {
    pub(crate) fn to_number(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        if !target.is_number() {
            let source_type = self.get_type();
            return Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)));
        }

        if self.get_type().is_number() {
            return self.number_to_number(target, ctx, span);
        }

        if self.is_bool() {
            return self.bool_to_number(target, span);
        }

        if self.is_utf8() {
            return match target {
                Type::Float4 | Type::Float8 => self.text_to_float(target, span),
                _ => self.text_to_integer(target, span),
            };
        }

        if self.is_float() {
            return self.float_to_integer(target, span);
        }

        let source_type = self.get_type();
        Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
    }
}

impl ColumnValues {
    fn bool_to_number(&self, target: Type, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
        macro_rules! bool_to_number {
            ($target_ty:ty, $true_val:expr, $false_val:expr) => {{
                |out: &mut ColumnValues, val: bool| {
                    out.push::<$target_ty>(if val { $true_val } else { $false_val })
                }
            }};
        }

        match self {
            ColumnValues::Bool(values, bitvec) => {
                // Check if target type is supported
                let converter = match target {
                    Type::Int1 => bool_to_number!(i8, 1i8, 0i8),
                    Type::Int2 => bool_to_number!(i16, 1i16, 0i16),
                    Type::Int4 => bool_to_number!(i32, 1i32, 0i32),
                    Type::Int8 => bool_to_number!(i64, 1i64, 0i64),
                    Type::Int16 => bool_to_number!(i128, 1i128, 0i128),
                    Type::Uint1 => bool_to_number!(u8, 1u8, 0u8),
                    Type::Uint2 => bool_to_number!(u16, 1u16, 0u16),
                    Type::Uint4 => bool_to_number!(u32, 1u32, 0u32),
                    Type::Uint8 => bool_to_number!(u64, 1u64, 0u64),
                    Type::Uint16 => bool_to_number!(u128, 1u128, 0u128),
                    Type::Float4 => bool_to_number!(f32, 1.0f32, 0.0f32),
                    Type::Float8 => bool_to_number!(f64, 1.0f64, 0.0f64),
                    _ => {
                        let source_type = self.get_type();
                        return Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)));
                    }
                };

                let mut out = ColumnValues::with_capacity(target, values.len());
                for (idx, &val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        converter(&mut out, val);
                    } else {
                        out.push_undefined();
                    }
                }
                Ok(out)
            }
            _ => {
                let source_type = self.get_type();
                Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
            },
        }
    }
}

impl ColumnValues {
    fn float_to_integer(&self, target: Type, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
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
                _ => {
                    let source_type = self.get_type();
                    Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
                },
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
                _ => {
                    let source_type = self.get_type();
                    Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
                },
            },
            _ => {
                let source_type = self.get_type();
                Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
            },
        }
    }
}

macro_rules! parse_and_push {
    (parse_int, $ty:ty, $target_type:expr, $out:expr, $temp_span:expr, $base_span:expr) => {{
        let result = parse_int::<$ty>($temp_span).map_err(|e| {
            reifydb_core::Error(cast::invalid_number($base_span.clone(), $target_type, e.diagnostic()))
        })?;
        $out.push::<$ty>(result);
    }};
    (parse_uint, $ty:ty, $target_type:expr, $out:expr, $temp_span:expr, $base_span:expr) => {{
        let result = parse_uint::<$ty>($temp_span).map_err(|e| {
            reifydb_core::Error(cast::invalid_number($base_span.clone(), $target_type, e.diagnostic()))
        })?;
        $out.push::<$ty>(result);
    }};
}

impl ColumnValues {
    fn text_to_integer(
        &self,
        target: Type,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        match self {
            ColumnValues::Utf8(values, bitvec) => {
                let base_span = span();
                let mut out = ColumnValues::with_capacity(target, values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        use reifydb_core::BorrowedSpan;
                        let temp_span =
                            BorrowedSpan::with_position(val, base_span.line(), base_span.column());

                        match target {
                            Type::Int1 => parse_and_push!(
                                parse_int,
                                i8,
                                Type::Int1,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Int2 => parse_and_push!(
                                parse_int,
                                i16,
                                Type::Int2,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Int4 => parse_and_push!(
                                parse_int,
                                i32,
                                Type::Int4,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Int8 => parse_and_push!(
                                parse_int,
                                i64,
                                Type::Int8,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Int16 => parse_and_push!(
                                parse_int,
                                i128,
                                Type::Int16,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Uint1 => parse_and_push!(
                                parse_uint,
                                u8,
                                Type::Uint1,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Uint2 => parse_and_push!(
                                parse_uint,
                                u16,
                                Type::Uint2,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Uint4 => parse_and_push!(
                                parse_uint,
                                u32,
                                Type::Uint4,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Uint8 => parse_and_push!(
                                parse_uint,
                                u64,
                                Type::Uint8,
                                out,
                                temp_span,
                                base_span
                            ),
                            Type::Uint16 => parse_and_push!(
                                parse_uint,
                                u128,
                                Type::Uint16,
                                out,
                                temp_span,
                                base_span
                            ),
                            _ => {
                                let source_type = self.get_type();
                                return Err(reifydb_core::Error(cast::unsupported_cast(base_span.clone(), source_type, target)));
                            }
                        }
                    } else {
                        out.push_undefined();
                    }
                }
                Ok(out)
            }
            _ => {
                let source_type = self.get_type();
                Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
            },
        }
    }
}

impl ColumnValues {
    fn text_to_float(
        &self,
        target: Type,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        if let ColumnValues::Utf8(values, bitvec) = self {
            // Create base span once for efficiency
            let base_span = span();
            let mut out = ColumnValues::with_capacity(target, values.len());
            for (idx, val) in values.iter().enumerate() {
                if bitvec.get(idx) {
                    // Create efficient borrowed span for parsing
                    use reifydb_core::BorrowedSpan;
                    let temp_span =
                        BorrowedSpan::with_position(val, base_span.line(), base_span.column());

                    match target {
                        Type::Float4 => {
                            out.push::<f32>(parse_float::<f32>(temp_span).map_err(|e| {
                                reifydb_core::Error(cast::invalid_number(
                                    base_span.clone(),
                                    Type::Float4,
                                    e.diagnostic(),
                                ))
                            })?)
                        }

                        Type::Float8 => {
                            out.push::<f64>(parse_float::<f64>(temp_span).map_err(|e| {
                                reifydb_core::Error(cast::invalid_number(
                                    base_span.clone(),
                                    Type::Float8,
                                    e.diagnostic(),
                                ))
                            })?)
                        }
                        _ => {
                            let source_type = self.get_type();
                            return Err(reifydb_core::Error(cast::unsupported_cast(base_span.clone(), source_type, target)));
                        }
                    }
                } else {
                    out.push_undefined();
                }
            }
            Ok(out)
        } else {
            let source_type = self.get_type();
            Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
        }
    }
}

macro_rules! float_to_int_vec {
    ($fn_name:ident, $float_ty:ty, $int_ty:ty, $target_type:expr, $min_val:expr, $max_val:expr) => {
        fn $fn_name(values: &[$float_ty], bitvec: &BitVec) -> crate::Result<ColumnValues> {
            let mut out = ColumnValues::with_capacity($target_type, values.len());
            for (idx, &val) in values.iter().enumerate() {
                if bitvec.get(idx) {
                    let truncated = val.trunc();
                    if truncated >= $min_val && truncated <= $max_val {
                        out.push::<$int_ty>(truncated as $int_ty);
                    } else {
                        out.push_undefined();
                    }
                } else {
                    out.push_undefined();
                }
            }
            Ok(out)
        }
    };
}

float_to_int_vec!(f32_to_i8_vec, f32, i8, Type::Int1, i8::MIN as f32, i8::MAX as f32);
float_to_int_vec!(f32_to_i16_vec, f32, i16, Type::Int2, i16::MIN as f32, i16::MAX as f32);
float_to_int_vec!(f32_to_i32_vec, f32, i32, Type::Int4, i32::MIN as f32, i32::MAX as f32);
float_to_int_vec!(f32_to_i64_vec, f32, i64, Type::Int8, i64::MIN as f32, i64::MAX as f32);
float_to_int_vec!(f32_to_i128_vec, f32, i128, Type::Int16, i128::MIN as f32, i128::MAX as f32);
float_to_int_vec!(f32_to_u8_vec, f32, u8, Type::Uint1, 0.0, u8::MAX as f32);
float_to_int_vec!(f32_to_u16_vec, f32, u16, Type::Uint2, 0.0, u16::MAX as f32);
float_to_int_vec!(f32_to_u32_vec, f32, u32, Type::Uint4, 0.0, u32::MAX as f32);
float_to_int_vec!(f32_to_u64_vec, f32, u64, Type::Uint8, 0.0, u64::MAX as f32);
float_to_int_vec!(f32_to_u128_vec, f32, u128, Type::Uint16, 0.0, u128::MAX as f32);

float_to_int_vec!(f64_to_i8_vec, f64, i8, Type::Int1, i8::MIN as f64, i8::MAX as f64);
float_to_int_vec!(f64_to_i16_vec, f64, i16, Type::Int2, i16::MIN as f64, i16::MAX as f64);
float_to_int_vec!(f64_to_i32_vec, f64, i32, Type::Int4, i32::MIN as f64, i32::MAX as f64);
float_to_int_vec!(f64_to_i64_vec, f64, i64, Type::Int8, i64::MIN as f64, i64::MAX as f64);
float_to_int_vec!(f64_to_i128_vec, f64, i128, Type::Int16, i128::MIN as f64, i128::MAX as f64);
float_to_int_vec!(f64_to_u8_vec, f64, u8, Type::Uint1, 0.0, u8::MAX as f64);
float_to_int_vec!(f64_to_u16_vec, f64, u16, Type::Uint2, 0.0, u16::MAX as f64);
float_to_int_vec!(f64_to_u32_vec, f64, u32, Type::Uint4, 0.0, u32::MAX as f64);
float_to_int_vec!(f64_to_u64_vec, f64, u64, Type::Uint8, 0.0, u64::MAX as f64);
float_to_int_vec!(f64_to_u128_vec, f64, u128, Type::Uint16, 0.0, u128::MAX as f64);

impl ColumnValues {
    fn number_to_number(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        if !target.is_number() {
            return Err(reifydb_core::Error(cast::unsupported_cast(
                span(),
                self.get_type(),
                target,
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

        let source_type = self.get_type();
        Err(reifydb_core::Error(cast::unsupported_cast(span(), source_type, target)))
    }
}

pub(crate) fn demote_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    demote: impl Demote,
    span: impl Fn() -> OwnedSpan,
    target_kind: Type,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeDemote<To>,
    To: GetType,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match demote.demote::<From, To>(val, &span)? {
                Some(v) => push(&mut out, v),
                None => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub(crate) fn promote_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    ctx: impl Promote,
    span: impl Fn() -> OwnedSpan,
    target_kind: Type,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafePromote<To>,
    To: GetType,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match ctx.promote::<From, To>(val, &span)? {
                Some(v) => push(&mut out, v),
                None => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

pub(crate) fn convert_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    ctx: impl Convert,
    span: impl Fn() -> OwnedSpan,
    target_kind: Type,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeConvert<To> + GetType,
    To: GetType,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match ctx.convert::<From, To>(val, &span)? {
                Some(v) => push(&mut out, v),
                None => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    mod promote {
        use crate::evaluate::Promote;
        use crate::frame::column::cast::number::promote_vec;
        use reifydb_core::value::number::SafePromote;
        use reifydb_core::{BitVec, Type};
        use reifydb_core::{IntoOwnedSpan, OwnedSpan};

        #[test]
        fn test_ok() {
            let values = [1i8, 2i8];
            let bitvec = BitVec::from_slice(&[true, true]);
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            let slice: &[i16] = result.as_slice();
            assert_eq!(slice, &[1i16, 2i16]);
        }

        #[test]
        fn test_none_maps_to_undefined() {
            // 42 mapped to None
            let values = [42i8];
            let bitvec = BitVec::from_slice(&[true]);
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert!(!result.bitvec().get(0));
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i8];
            let bitvec = BitVec::from_slice(&[false]);
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert!(!result.bitvec().get(0));
        }

        #[test]
        fn test_mixed_bitvec_and_promote_failure() {
            let values = [1i8, 42i8, 3i8, 4i8];
            let bitvec = BitVec::from_slice(&[true, true, false, true]);
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            let slice = result.as_slice::<i16>();
            assert_eq!(slice, &[1i16, 0, 0, 4i16]);
            assert!(result.bitvec().get(0));
            assert!(!result.bitvec().get(1));
            assert!(!result.bitvec().get(2));
            assert!(result.bitvec().get(3));
        }

        struct TestCtx;

        impl TestCtx {
            fn new() -> Self {
                Self
            }
        }

        impl Promote for &TestCtx {
            /// Can only used with i8
            fn promote<From, To>(
                &self,
                val: From,
                _span: impl IntoOwnedSpan,
            ) -> crate::evaluate::Result<Option<To>>
            where
                From: SafePromote<To>,
            {
                // Only simulate promotion failure for i8 == 42
                let raw: i8 = unsafe { std::mem::transmute_copy(&val) };
                if raw == 42 {
                    return Ok(None);
                }
                Ok(Some(val.checked_promote().unwrap()))
            }
        }
    }

    mod demote {
        use crate::evaluate::Demote;
        use crate::frame::column::cast::number::demote_vec;
        use reifydb_core::value::number::SafeDemote;
        use reifydb_core::{BitVec, Type};
        use reifydb_core::{IntoOwnedSpan, OwnedSpan};

        #[test]
        fn test_ok() {
            let values = [1i16, 2i16];
            let bitvec = BitVec::from_slice(&[true, true]);
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            let slice: &[i8] = result.as_slice();
            assert_eq!(slice, &[1i8, 2i8]);
            assert!(result.bitvec().get(0));
            assert!(result.bitvec().get(1));
        }

        #[test]
        fn test_none_maps_to_undefined() {
            let values = [42i16];
            let bitvec = BitVec::from_slice(&[true]);
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert!(!result.bitvec().get(0));
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i16];
            let bitvec = BitVec::new(1, false);
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert!(!result.bitvec().get(0));
        }

        #[test]
        fn test_mixed_bitvec_and_demote_failure() {
            let values = [1i16, 42i16, 3i16, 4i16];
            let bitvec = BitVec::from_slice(&[true, true, false, true]);
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &bitvec,
                &ctx,
                || OwnedSpan::testing_empty(),
                Type::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            let slice: &[i8] = result.as_slice();
            assert_eq!(slice, &[1i8, 0, 0, 4i8]);
            assert!(result.bitvec().get(0));
            assert!(!result.bitvec().get(1));
            assert!(!result.bitvec().get(2));
            assert!(result.bitvec().get(3));
        }

        struct TestCtx;

        impl TestCtx {
            fn new() -> Self {
                Self
            }
        }

        impl Demote for &TestCtx {
            /// Can only be used with i16 → i8
            fn demote<From, To>(
                &self,
                val: From,
                _span: impl IntoOwnedSpan,
            ) -> crate::evaluate::Result<Option<To>>
            where
                From: SafeDemote<To>,
            {
                // Only simulate promotion failure for i16 == 42
                let raw: i16 = unsafe { std::mem::transmute_copy(&val) };
                if raw == 42 {
                    return Ok(None);
                }
                Ok(Some(val.checked_demote().unwrap()))
            }
        }
    }
}
