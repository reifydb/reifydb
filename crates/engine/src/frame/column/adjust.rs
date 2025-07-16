// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::evaluate::{Convert, Demote, Promote};
use crate::evaluate::constant::date::parse_date;
use crate::evaluate::constant::datetime::parse_datetime;
use crate::evaluate::constant::time::parse_time;
use crate::evaluate::constant::interval::parse_interval;
use crate::frame::ColumnValues;
use reifydb_core::DataType;
use reifydb_core::num::{SafeConvert, SafeDemote, SafePromote, parse_int, parse_uint, parse_float};
use reifydb_core::{Span, Date, DateTime, Time, Interval};
use std::fmt::Display;

impl ColumnValues {
    pub fn adjust(
		&self,
		target: DataType,
		ctx: impl Promote + Demote + Convert,
		span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        use DataType::*;

        if target == self.data_type() {
            return Ok(self.clone());
        }
        
        macro_rules! adjust {
        (
            $src_variant:ident, $src_ty:ty,
            promote => [ $( ($pro_variant:ident, $pro_ty:ty) ),* ],
            demote => [ $( ($dem_variant:ident, $dem_ty:ty) ),* ],
            convert => [ $( ($con_variant:ident, $con_ty:ty) ),* ]
        ) => {
            if let ColumnValues::$src_variant(values, validity) = self {
                match target {
                    $(
                        $pro_variant => return promote_vec::<$src_ty, $pro_ty>(
                            values,
                            validity,
                            ctx,
                            &span,
                            $pro_variant,
                            ColumnValues::push::<$pro_ty>,
                        ),
                    )*
                    $(
                        $dem_variant => return demote_vec::<$src_ty, $dem_ty>(
                                values,
                                validity,
                                ctx,
                                &span,
                                $dem_variant,
                                ColumnValues::push::<$dem_ty>,
                            ),
                    )*
                    $(
                        $con_variant => return convert_vec::<$src_ty, $con_ty>(
                            values,
                            validity,
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
            convert => [ ]
        );

        adjust!(Float8, f64,
            promote => [ ],
            demote => [(Float4, f32)],
            convert => [ ]
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
        if let ColumnValues::Bool(values, validity) = self {
            match target {
                DataType::Bool => return Ok(self.clone()),
                DataType::Int1 => return bool_to_numeric_vec(values, validity, target),
                DataType::Int2 => return bool_to_numeric_vec(values, validity, target),
                DataType::Int4 => return bool_to_numeric_vec(values, validity, target),
                DataType::Int8 => return bool_to_numeric_vec(values, validity, target),
                DataType::Int16 => return bool_to_numeric_vec(values, validity, target),
                DataType::Uint1 => return bool_to_numeric_vec(values, validity, target),
                DataType::Uint2 => return bool_to_numeric_vec(values, validity, target),
                DataType::Uint4 => return bool_to_numeric_vec(values, validity, target),
                DataType::Uint8 => return bool_to_numeric_vec(values, validity, target),
                DataType::Uint16 => return bool_to_numeric_vec(values, validity, target),
                DataType::Float4 => return bool_to_numeric_vec(values, validity, target),
                DataType::Float8 => return bool_to_numeric_vec(values, validity, target),
                DataType::Utf8 => return bool_to_text_vec(values, validity),
                _ => {}
            }
        }

        // Handle Utf8 conversions  
        if let ColumnValues::Utf8(values, validity) = self {
            match target {
                DataType::Utf8 => return Ok(self.clone()),
                DataType::Bool => return text_to_bool_vec(values, validity),
                DataType::Int1 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Int2 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Int4 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Int8 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Int16 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Uint1 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Uint2 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Uint4 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Uint8 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Uint16 => return text_to_numeric_vec(values, validity, target, &span),
                DataType::Float4 => return text_to_float_vec(values, validity, target, &span),
                DataType::Float8 => return text_to_float_vec(values, validity, target, &span),
                DataType::Date => return text_to_date_vec(values, validity, &span),
                DataType::DateTime => return text_to_datetime_vec(values, validity, &span),
                DataType::Time => return text_to_time_vec(values, validity, &span),
                DataType::Interval => return text_to_interval_vec(values, validity, &span),
                _ => {}
            }
        }

        // Handle numeric to Bool conversions
        match target {
            DataType::Bool => {
                match self {
                    ColumnValues::Int1(values, validity) => return i8_to_bool_vec(values, validity),
                    ColumnValues::Int2(values, validity) => return i16_to_bool_vec(values, validity),
                    ColumnValues::Int4(values, validity) => return i32_to_bool_vec(values, validity),
                    ColumnValues::Int8(values, validity) => return i64_to_bool_vec(values, validity),
                    ColumnValues::Int16(values, validity) => return i128_to_bool_vec(values, validity),
                    ColumnValues::Uint1(values, validity) => return u8_to_bool_vec(values, validity),
                    ColumnValues::Uint2(values, validity) => return u16_to_bool_vec(values, validity),
                    ColumnValues::Uint4(values, validity) => return u32_to_bool_vec(values, validity),
                    ColumnValues::Uint8(values, validity) => return u64_to_bool_vec(values, validity),
                    ColumnValues::Uint16(values, validity) => return u128_to_bool_vec(values, validity),
                    ColumnValues::Float4(values, validity) => return f32_to_bool_vec(values, validity),
                    ColumnValues::Float8(values, validity) => return f64_to_bool_vec(values, validity),
                    _ => {}
                }
            }
            DataType::Utf8 => {
                match self {
                    ColumnValues::Int1(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Int2(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Int4(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Int8(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Int16(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Uint1(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Uint2(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Uint4(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Uint8(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Uint16(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Float4(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Float8(values, validity) => return numeric_to_text_vec(values, validity),
                    ColumnValues::Date(values, validity) => return date_to_text_vec(values, validity),
                    ColumnValues::DateTime(values, validity) => return datetime_to_text_vec(values, validity),
                    ColumnValues::Time(values, validity) => return time_to_text_vec(values, validity),
                    ColumnValues::Interval(values, validity) => return interval_to_text_vec(values, validity),
                    _ => {}
                }
            }
            _ => {}
        }

        // Handle Float to integer/unsigned conversions
        if let ColumnValues::Float4(values, validity) = self {
            match target {
                DataType::Int1 => return f32_to_i8_vec(values, validity),
                DataType::Int2 => return f32_to_i16_vec(values, validity),
                DataType::Int4 => return f32_to_i32_vec(values, validity),
                DataType::Int8 => return f32_to_i64_vec(values, validity),
                DataType::Int16 => return f32_to_i128_vec(values, validity),
                DataType::Uint1 => return f32_to_u8_vec(values, validity),
                DataType::Uint2 => return f32_to_u16_vec(values, validity),
                DataType::Uint4 => return f32_to_u32_vec(values, validity),
                DataType::Uint8 => return f32_to_u64_vec(values, validity),
                DataType::Uint16 => return f32_to_u128_vec(values, validity),
                _ => {}
            }
        }

        if let ColumnValues::Float8(values, validity) = self {
            match target {
                DataType::Int1 => return f64_to_i8_vec(values, validity),
                DataType::Int2 => return f64_to_i16_vec(values, validity),
                DataType::Int4 => return f64_to_i32_vec(values, validity),
                DataType::Int8 => return f64_to_i64_vec(values, validity),
                DataType::Int16 => return f64_to_i128_vec(values, validity),
                DataType::Uint1 => return f64_to_u8_vec(values, validity),
                DataType::Uint2 => return f64_to_u16_vec(values, validity),
                DataType::Uint4 => return f64_to_u32_vec(values, validity),
                DataType::Uint8 => return f64_to_u64_vec(values, validity),
                DataType::Uint16 => return f64_to_u128_vec(values, validity),
                _ => {}
            }
        }

        unimplemented!("{:?} -> {:?}", self.data_type(), target)
    }
}

fn demote_vec<From, To>(
	values: &[From],
	validity: &[bool],
	demote: impl Demote,
	span: impl Fn() -> Span,
	target_kind: DataType,
	mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeDemote<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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
	validity: &[bool],
	ctx: impl Promote,
	span: impl Fn() -> Span,
	target_kind: DataType,
	mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafePromote<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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
	validity: &[bool],
	ctx: impl Convert,
	span: impl Fn() -> Span,
	target_kind: DataType,
	mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeConvert<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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
        use reifydb_core::DataType;
        use reifydb_core::num::SafePromote;
        use reifydb_core::{IntoSpan, Span};

        #[test]
        fn test_ok() {
            let values = [1i8, 2i8];
            let validity = [true, true];
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int2,
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
            let validity = [true];
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int2,
				|col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i8];
            let validity = [false];
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int2,
				|col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_mixed_validity_and_promote_failure() {
            let values = [1i8, 42i8, 3i8, 4i8];
            let validity = [true, true, false, true];
            let ctx = TestCtx::new();

            let result = promote_vec::<i8, i16>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int2,
				|col, v| col.push::<i16>(v),
            )
            .unwrap();

            let slice = result.as_slice::<i16>();
            assert_eq!(slice, &[1i16, 0, 0, 4i16]);
            assert_eq!(result.validity(), &[true, false, false, true]);
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
        use reifydb_core::DataType;
        use reifydb_core::num::SafeDemote;
        use reifydb_core::{IntoSpan, Span};

        #[test]
        fn test_ok() {
            let values = [1i16, 2i16];
            let validity = [true, true];
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int1,
				|col, v| col.push::<i8>(v),
            )
            .unwrap();

            let slice: &[i8] = result.as_slice();
            assert_eq!(slice, &[1i8, 2i8]);
            assert_eq!(result.validity(), &[true, true]);
        }

        #[test]
        fn test_none_maps_to_undefined() {
            let values = [42i16];
            let validity = [true];
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int1,
				|col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i16];
            let validity = [false];
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int1,
				|col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_mixed_validity_and_demote_failure() {
            let values = [1i16, 42i16, 3i16, 4i16];
            let validity = [true, true, false, true];
            let ctx = TestCtx::new();

            let result = demote_vec::<i16, i8>(
				&values,
				&validity,
				&ctx,
				|| Span::testing_empty(),
				DataType::Int1,
				|col, v| col.push::<i8>(v),
            )
            .unwrap();

            let slice: &[i8] = result.as_slice();
            assert_eq!(slice, &[1i8, 0, 0, 4i8]);
            assert_eq!(result.validity(), &[true, false, false, true]);
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
    validity: &[bool],
    target: DataType,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(target, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            match target {
                DataType::Int1 => out.push::<i8>(if val { 1i8 } else { 0i8 }),
                DataType::Int2 => out.push::<i16>(if val { 1i16 } else { 0i16 }),
                DataType::Int4 => out.push::<i32>(if val { 1i32 } else { 0i32 }),
                DataType::Int8 => out.push::<i64>(if val { 1i64 } else { 0i64 }),
                DataType::Int16 => out.push::<i128>(if val { 1i128 } else { 0i128 }),
                DataType::Uint1 => out.push::<u8>(if val { 1u8 } else { 0u8 }),
                DataType::Uint2 => out.push::<u16>(if val { 1u16 } else { 0u16 }),
                DataType::Uint4 => out.push::<u32>(if val { 1u32 } else { 0u32 }),
                DataType::Uint8 => out.push::<u64>(if val { 1u64 } else { 0u64 }),
                DataType::Uint16 => out.push::<u128>(if val { 1u128 } else { 0u128 }),
                DataType::Float4 => out.push::<f32>(if val { 1.0f32 } else { 0.0f32 }),
                DataType::Float8 => out.push::<f64>(if val { 1.0f64 } else { 0.0f64 }),
                _ => unreachable!(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn bool_to_text_vec(
    values: &[bool],
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<String>(if val { "true".to_string() } else { "false".to_string() });
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

// Specific implementations for different numeric types
fn i8_to_bool_vec(values: &[i8], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i16_to_bool_vec(values: &[i16], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i32_to_bool_vec(values: &[i32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i64_to_bool_vec(values: &[i64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn i128_to_bool_vec(values: &[i128], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u8_to_bool_vec(values: &[u8], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u16_to_bool_vec(values: &[u16], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u32_to_bool_vec(values: &[u32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u64_to_bool_vec(values: &[u64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn u128_to_bool_vec(values: &[u128], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn f32_to_bool_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0.0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn f64_to_bool_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<bool>(val != 0.0);
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn numeric_to_text_vec<T>(
    values: &[T],
    validity: &[bool],
) -> crate::Result<ColumnValues>
where
    T: Copy + Display,
{
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
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
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Bool, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            match val.as_str() {
                "true" => out.push::<bool>(true),
                "false" => out.push::<bool>(false),
                _ => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_numeric_vec(
    values: &[String],
    validity: &[bool],
    target: DataType,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(target, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            // Create a temporary span for parsing
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            // Try to parse based on the target type
            match target {
                DataType::Int1 => {
                    match parse_int::<i8>(&temp_span) {
                        Ok(v) => out.push::<i8>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= i8::MIN as f64 && truncated <= i8::MAX as f64 {
                                    out.push::<i8>(truncated as i8);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Int2 => {
                    match parse_int::<i16>(&temp_span) {
                        Ok(v) => out.push::<i16>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= i16::MIN as f64 && truncated <= i16::MAX as f64 {
                                    out.push::<i16>(truncated as i16);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Int4 => {
                    match parse_int::<i32>(&temp_span) {
                        Ok(v) => out.push::<i32>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= i32::MIN as f64 && truncated <= i32::MAX as f64 {
                                    out.push::<i32>(truncated as i32);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Int8 => {
                    match parse_int::<i64>(&temp_span) {
                        Ok(v) => out.push::<i64>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                                    out.push::<i64>(truncated as i64);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Int16 => {
                    match parse_int::<i128>(&temp_span) {
                        Ok(v) => out.push::<i128>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= i128::MIN as f64 && truncated <= i128::MAX as f64 {
                                    out.push::<i128>(truncated as i128);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Uint1 => {
                    match parse_uint::<u8>(&temp_span) {
                        Ok(v) => out.push::<u8>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= 0.0 && truncated <= u8::MAX as f64 {
                                    out.push::<u8>(truncated as u8);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Uint2 => {
                    match parse_uint::<u16>(&temp_span) {
                        Ok(v) => out.push::<u16>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= 0.0 && truncated <= u16::MAX as f64 {
                                    out.push::<u16>(truncated as u16);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Uint4 => {
                    match parse_uint::<u32>(&temp_span) {
                        Ok(v) => out.push::<u32>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= 0.0 && truncated <= u32::MAX as f64 {
                                    out.push::<u32>(truncated as u32);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Uint8 => {
                    match parse_uint::<u64>(&temp_span) {
                        Ok(v) => out.push::<u64>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= 0.0 && truncated <= u64::MAX as f64 {
                                    out.push::<u64>(truncated as u64);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
                DataType::Uint16 => {
                    match parse_uint::<u128>(&temp_span) {
                        Ok(v) => out.push::<u128>(v),
                        Err(_) => {
                            if let Ok(f) = parse_float::<f64>(&temp_span) {
                                let truncated = f.trunc();
                                if truncated >= 0.0 && truncated <= u128::MAX as f64 {
                                    out.push::<u128>(truncated as u128);
                                } else {
                                    out.push_undefined();
                                }
                            } else {
                                out.push_undefined();
                            }
                        }
                    }
                }
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
    validity: &[bool],
    target: DataType,
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(target, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            match target {
                DataType::Float4 => {
                    match parse_float::<f32>(&temp_span) {
                        Ok(v) => out.push::<f32>(v),
                        Err(_) => out.push_undefined(),
                    }
                }
                DataType::Float8 => {
                    match parse_float::<f64>(&temp_span) {
                        Ok(v) => out.push::<f64>(v),
                        Err(_) => out.push_undefined(),
                    }
                }
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
    validity: &[bool],
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Date, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            match parse_date(&temp_span) {
                Ok(date) => out.push::<Date>(date),
                Err(_) => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_datetime_vec(
    values: &[String],
    validity: &[bool],
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::DateTime, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            match parse_datetime(&temp_span) {
                Ok(datetime) => out.push::<DateTime>(datetime),
                Err(_) => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_time_vec(
    values: &[String],
    validity: &[bool],
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Time, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            match parse_time(&temp_span) {
                Ok(time) => out.push::<Time>(time),
                Err(_) => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn text_to_interval_vec(
    values: &[String],
    validity: &[bool],
    span: impl Fn() -> Span,
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Interval, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            let temp_span = Span {
                fragment: val.clone(),
                line: span().line,
                column: span().column,
            };
            
            match parse_interval(&temp_span) {
                Ok(interval) => out.push::<Interval>(interval),
                Err(_) => out.push_undefined(),
            }
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn date_to_text_vec(
    values: &[Date],
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<String>(val.to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn datetime_to_text_vec(
    values: &[DateTime],
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<String>(val.to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn time_to_text_vec(
    values: &[Time],
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<String>(val.to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

fn interval_to_text_vec(
    values: &[Interval],
    validity: &[bool],
) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Utf8, values.len());
    for (idx, val) in values.iter().enumerate() {
        if validity[idx] {
            out.push::<String>(val.to_string());
        } else {
            out.push_undefined();
        }
    }
    Ok(out)
}

// Float32 to integer conversion functions
fn f32_to_i8_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_i16_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_i32_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_i64_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_i128_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_u8_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_u16_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_u32_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_u64_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f32_to_u128_vec(values: &[f32], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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
fn f64_to_i8_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_i16_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_i32_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_i64_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_i128_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Int16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_u8_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint1, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_u16_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint2, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_u32_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint4, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_u64_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint8, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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

fn f64_to_u128_vec(values: &[f64], validity: &[bool]) -> crate::Result<ColumnValues> {
    let mut out = ColumnValues::with_capacity(DataType::Uint16, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
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
