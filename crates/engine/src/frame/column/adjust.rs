// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::error;
use crate::evaluate::{Convert, Demote, Error, Promote};
use crate::frame::ColumnValues;
use reifydb_core::diagnostic::boolean::invalid_numeric_boolean;
use reifydb_core::diagnostic::cast;
use reifydb_core::value::boolean::parse_bool;
use reifydb_core::value::number::{
    SafeConvert, SafeDemote, SafePromote, parse_float, parse_int, parse_uint,
};
use reifydb_core::value::temporal::{parse_date, parse_datetime, parse_interval, parse_time};
use reifydb_core::{BitVec, GetType, Type};
use reifydb_core::{Date, DateTime, Interval, Span, Time};
use std::fmt::Display;

impl ColumnValues {
    pub fn adjust(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        use Type::*;

        if target == self.ty() {
            return Ok(self.clone());
        }

        macro_rules! adjust {
        (
            $src_variant:ident, $src_ty:ty,
            promote => [ $( ($pro_variant:ident, $pro_ty:ty) ),* ],
            demote => [ $( ($dem_variant:ident, $dem_ty:ty) ),* ],
            convert => [ $( ($con_variant:ident, $con_ty:ty) ),* ]
        ) => {
            if let ColumnValues::$src_variant(values, bitvec) = self {
                match target {
                    $(
                        $pro_variant => return promote_vec::<$src_ty, $pro_ty>(
                            values,
                            bitvec,
                            ctx,
                            &span,
                            $pro_variant,
                            ColumnValues::push::<$pro_ty>,
                        ),
                    )*
                    $(
                        $dem_variant => return demote_vec::<$src_ty, $dem_ty>(
                                values,
                                bitvec,
                                ctx,
                                &span,
                                $dem_variant,
                                ColumnValues::push::<$dem_ty>,
                            ),
                    )*
                    $(
                        $con_variant => return convert_vec::<$src_ty, $con_ty>(
                            values,
                            bitvec,
                            ctx,
                            &span,
                            $con_variant,
                            ColumnValues::push::<$con_ty>,
                        ),
                    )*
                    _ => {}
                }
            }
        }
    }

        adjust!(Float4, f32,
            promote => [(Float8, f64) ],
            demote => [ ],
            convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Float8, f64,
            promote => [ ],
            demote => [(Float4, f32)],
            convert => [(Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int1, i8,
            promote => [(Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)],
            demote => [],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int2, i16,
            promote => [(Int4, i32), (Int8, i64), (Int16, i128)],
            demote => [(Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int4, i32,
            promote => [(Int8, i64), (Int16, i128)],
            demote => [(Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int8, i64,
            promote => [(Int16, i128)],
            demote => [(Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int16, i128,
            promote => [],
            demote => [(Int8, i64), (Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint1, u8), (Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Uint1, u8,
            promote => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint2, u16,
            promote => [(Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [(Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint4, u32,
            promote => [(Uint8, u64), (Uint16, u128)],
            demote => [(Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint8, u64,
            promote => [(Uint16, u128)],
            demote => [(Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint16, u128,
            promote => [],
            demote => [(Uint8, u64), (Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        // Handle Bool conversions
        if let ColumnValues::Bool(values, bitvec) = self {
            match target {
                Type::Bool => return Ok(self.clone()),
                Type::Int1 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Int2 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Int4 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Int8 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Int16 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Uint1 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Uint2 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Uint4 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Uint8 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Uint16 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Float4 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Float8 => return bool_to_numeric_vec(values, bitvec, target),
                Type::Utf8 => return bool_to_text_vec(values, bitvec),
                _ => {}
            }
        }

        // Handle Utf8 conversions
        if let ColumnValues::Utf8(values, bitvec) = self {
            match target {
                Type::Utf8 => return Ok(self.clone()),
                Type::Bool => return text_to_bool_vec(values, bitvec, &span),
                Type::Int1 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Int2 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Int4 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Int8 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Int16 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Uint1 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Uint2 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Uint4 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Uint8 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Uint16 => return text_to_numeric_vec(values, bitvec, target, &span),
                Type::Float4 => return text_to_float_vec(values, bitvec, target, &span),
                Type::Float8 => return text_to_float_vec(values, bitvec, target, &span),
                Type::Date => return text_to_date_vec(values, bitvec, &span),
                Type::DateTime => return text_to_datetime_vec(values, bitvec, &span),
                Type::Time => return text_to_time_vec(values, bitvec, &span),
                Type::Interval => return text_to_interval_vec(values, bitvec, &span),
                _ => {}
            }
        }

        // Handle numeric to Bool conversions
        match target {
            Type::Bool => match self {
                ColumnValues::Int1(values, bitvec) => return i8_to_bool_vec(values, bitvec, &span),
                ColumnValues::Int2(values, bitvec) => {
                    return i16_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Int4(values, bitvec) => {
                    return i32_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Int8(values, bitvec) => {
                    return i64_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Int16(values, bitvec) => {
                    return i128_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Uint1(values, bitvec) => {
                    return u8_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Uint2(values, bitvec) => {
                    return u16_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Uint4(values, bitvec) => {
                    return u32_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Uint8(values, bitvec) => {
                    return u64_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Uint16(values, bitvec) => {
                    return u128_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Float4(values, bitvec) => {
                    return f32_to_bool_vec(values, bitvec, &span);
                }
                ColumnValues::Float8(values, bitvec) => {
                    return f64_to_bool_vec(values, bitvec, &span);
                }
                _ => {}
            },
            Type::Utf8 => match self {
                ColumnValues::Int1(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Int2(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Int4(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Int8(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Int16(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Uint1(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Uint2(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Uint4(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Uint8(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Uint16(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Float4(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Float8(values, bitvec) => {
                    return numeric_to_text_vec(values, bitvec);
                }
                ColumnValues::Date(values, bitvec) => return date_to_text_vec(values, bitvec),
                ColumnValues::DateTime(values, bitvec) => {
                    return datetime_to_text_vec(values, bitvec);
                }
                ColumnValues::Time(values, bitvec) => return time_to_text_vec(values, bitvec),
                ColumnValues::Interval(values, bitvec) => {
                    return interval_to_text_vec(values, bitvec);
                }
                _ => {}
            },
            _ => {}
        }

        // Handle Float to integer/unsigned conversions
        if let ColumnValues::Float4(values, bitvec) = self {
            match target {
                Type::Int1 => return f32_to_i8_vec(values, bitvec),
                Type::Int2 => return f32_to_i16_vec(values, bitvec),
                Type::Int4 => return f32_to_i32_vec(values, bitvec),
                Type::Int8 => return f32_to_i64_vec(values, bitvec),
                Type::Int16 => return f32_to_i128_vec(values, bitvec),
                Type::Uint1 => return f32_to_u8_vec(values, bitvec),
                Type::Uint2 => return f32_to_u16_vec(values, bitvec),
                Type::Uint4 => return f32_to_u32_vec(values, bitvec),
                Type::Uint8 => return f32_to_u64_vec(values, bitvec),
                Type::Uint16 => return f32_to_u128_vec(values, bitvec),
                _ => {}
            }
        }

        if let ColumnValues::Float8(values, bitvec) = self {
            match target {
                Type::Int1 => return f64_to_i8_vec(values, bitvec),
                Type::Int2 => return f64_to_i16_vec(values, bitvec),
                Type::Int4 => return f64_to_i32_vec(values, bitvec),
                Type::Int8 => return f64_to_i64_vec(values, bitvec),
                Type::Int16 => return f64_to_i128_vec(values, bitvec),
                Type::Uint1 => return f64_to_u8_vec(values, bitvec),
                Type::Uint2 => return f64_to_u16_vec(values, bitvec),
                Type::Uint4 => return f64_to_u32_vec(values, bitvec),
                Type::Uint8 => return f64_to_u64_vec(values, bitvec),
                Type::Uint16 => return f64_to_u128_vec(values, bitvec),
                _ => {}
            }
        }

        unimplemented!("{:?} -> {:?}", self.ty(), target)
    }
}

fn demote_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    demote: impl Demote,
    span: impl Fn() -> Span,
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

fn promote_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    ctx: impl Promote,
    span: impl Fn() -> Span,
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

fn convert_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    ctx: impl Convert,
    span: impl Fn() -> Span,
    target_kind: Type,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeConvert<To>,
    To: GetType
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
        use crate::frame::column::adjust::promote_vec;
        use reifydb_core::value::number::SafePromote;
        use reifydb_core::{BitVec, Type};
        use reifydb_core::{IntoSpan, Span};

        #[test]
        fn test_ok() {
            let values = [1i8, 2i8];
            let bitvec = BitVec::from_slice(&[true, true]);
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &bitvec,
                &ctx,
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                _span: impl IntoSpan,
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
        use crate::frame::column::adjust::demote_vec;
        use reifydb_core::value::number::SafeDemote;
        use reifydb_core::{BitVec, Type};
        use reifydb_core::{IntoSpan, Span};

        #[test]
        fn test_ok() {
            let values = [1i16, 2i16];
            let bitvec = BitVec::from_slice(&[true, true]);
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &bitvec,
                &ctx,
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                || Span::testing_empty(),
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
                _span: impl IntoSpan,
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

// Bool conversion functions
fn bool_to_numeric_vec(
    values: &[bool],
    bitvec: &BitVec,
    target: Type,
) -> crate::Result<ColumnValues> {
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

fn bool_to_text_vec(values: &[bool], bitvec: &BitVec) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Utf8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            out.push::<String>(if val { "true".to_string() } else { "false".to_string() });
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

// Specific implementations for different numeric types
fn i8_to_bool_vec(
    values: &[i8],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i16_to_bool_vec(
    values: &[i16],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i32_to_bool_vec(
    values: &[i32],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i64_to_bool_vec(
    values: &[i64],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i128_to_bool_vec(
    values: &[i128],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u8_to_bool_vec(
    values: &[u8],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u16_to_bool_vec(
    values: &[u16],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u32_to_bool_vec(
    values: &[u32],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u64_to_bool_vec(
    values: &[u64],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u128_to_bool_vec(
    values: &[u128],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            match val {
                0 => out.push::<bool>(false),
                1 => out.push::<bool>(true),
                _ => {
                    let mut error_span = span();
                    error_span.fragment = val.to_string();
                    return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(
                        error_span,
                    ))));
                }
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn f32_to_bool_vec(
    values: &[f32],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            if val == 0.0 {
                out.push::<bool>(false);
            } else if val == 1.0 {
                out.push::<bool>(true);
            } else {
                let mut error_span = span();
                error_span.fragment = val.to_string();
                return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(error_span))));
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn f64_to_bool_vec(
    values: &[f64],
    bitvec: &BitVec,
    span: &impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            if val == 0.0 {
                out.push::<bool>(false);
            } else if val == 1.0 {
                out.push::<bool>(true);
            } else {
                let mut error_span = span();
                error_span.fragment = val.to_string();
                return Err(error::Error::Evaluation(Error(invalid_numeric_boolean(error_span))));
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn numeric_to_text_vec<T>(values: &[T], bitvec: &BitVec) -> crate::Result<ColumnValues>
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

// Text parsing functions
fn text_to_bool_vec(
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Bool, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let mut span = span();
            span.fragment = val.clone();

            out.push(parse_bool(&mut span)?);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_numeric_vec(
    values: &[String],
    bitvec: &BitVec,
    target: Type,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(target, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            // Create a temporary span for parsing
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            // Try to parse based on the target type
            match target {
                Type::Int1 => out.push::<i8>(parse_int::<i8>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Int1, e.diagnostic()))
                })?),
                Type::Int2 => out.push::<i16>(parse_int::<i16>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Int2, e.diagnostic()))
                })?),
                Type::Int4 => out.push::<i32>(parse_int::<i32>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Int4, e.diagnostic()))
                })?),
                Type::Int8 => out.push::<i64>(parse_int::<i64>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Int8, e.diagnostic()))
                })?),
                Type::Int16 => out.push::<i128>(parse_int::<i128>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Int16, e.diagnostic()))
                })?),
                Type::Uint1 => out.push::<u8>(parse_uint::<u8>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Uint1, e.diagnostic()))
                })?),
                Type::Uint2 => out.push::<u16>(parse_uint::<u16>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Uint2, e.diagnostic()))
                })?),
                Type::Uint4 => out.push::<u32>(parse_uint::<u32>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Uint4, e.diagnostic()))
                })?),
                Type::Uint8 => out.push::<u64>(parse_uint::<u64>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Uint8, e.diagnostic()))
                })?),
                Type::Uint16 => out.push::<u128>(parse_uint::<u128>(&temp_span).map_err(|e| {
                    Error(cast::invalid_number(span(), Type::Uint16, e.diagnostic()))
                })?),
                _ => unreachable!(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_float_vec(
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

fn text_to_date_vec(
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Date, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            let date = parse_date(&temp_span)
                .map_err(|e| Error(cast::invalid_temporal(span(), Type::Date, e.0)))?;

            out.push::<Date>(date);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_datetime_vec(
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::DateTime, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            let datetime = parse_datetime(&temp_span)
                .map_err(|e| Error(cast::invalid_temporal(span(), Type::DateTime, e.0)))?;

            out.push::<DateTime>(datetime);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_time_vec(
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Time, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            let time = parse_time(&temp_span)
                .map_err(|e| Error(cast::invalid_temporal(span(), Type::Time, e.0)))?;

            out.push::<Time>(time);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_interval_vec(
    values: &[String],
    bitvec: &BitVec,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(Type::Interval, values.len());
    for (idx, val) in values.iter().enumerate() {
        if bitvec.get(idx) {
            let temp_span =
                Span { fragment: val.clone(), line: span().line, column: span().column };

            let interval = parse_interval(&temp_span)
                .map_err(|e| Error(cast::invalid_temporal(span(), Type::Interval, e.0)))?;

            out.push::<Interval>(interval);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn date_to_text_vec(values: &[Date], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn datetime_to_text_vec(values: &[DateTime], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn time_to_text_vec(values: &[Time], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn interval_to_text_vec(values: &[Interval], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

// Float32 to integer conversion functions
fn f32_to_i8_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_i16_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_i32_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_i64_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_i128_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_u8_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_u16_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_u32_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_u64_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f32_to_u128_vec(values: &[f32], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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
fn f64_to_i8_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_i16_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_i32_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_i64_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_i128_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_u8_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_u16_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_u32_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_u64_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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

fn f64_to_u128_vec(values: &[f64], bitvec: &BitVec) -> crate::Result<ColumnValues> {
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
