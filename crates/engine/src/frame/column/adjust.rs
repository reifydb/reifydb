// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::evaluate::{Convert, Demote, Promote};
use crate::frame::ColumnValues;
use reifydb_core::Kind;
use reifydb_core::num::{SafeConvert, SafeDemote, SafePromote};
use reifydb_diagnostic::Span;

impl ColumnValues {
    pub fn adjust_column(
        &self,
        target: Kind,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        use Kind::*;

        if target == self.kind() {
            return Ok(self.clone());
        }

        macro_rules! adjust {
        (
            $src_variant:ident, $src_ty:ty,
            promote => [ $( ($tgt_variant:ident, $tgt_ty:ty) ),* ],
            demote => [ $( ($dem_tgt_variant:ident, $dem_tgt_ty:ty) ),* ],
            convert => [ $( ($conv_tgt_variant:ident, $conv_tgt_ty:ty) ),* ]
        ) => {
            if let ColumnValues::$src_variant(values, validity) = self {
                match target {
                    $(
                        $tgt_variant => return promote_vec::<$src_ty, $tgt_ty>(
                            values,
                            validity,
                            ctx,
                            &span,
                            $tgt_variant,
                            ColumnValues::push::<$tgt_ty>,
                        ),
                    )*
                    $(
                        $dem_tgt_variant => return demote_vec::<$src_ty, $dem_tgt_ty>(
                                values,
                                validity,
                                ctx,
                                &span,
                                $dem_tgt_variant,
                                ColumnValues::push::<$dem_tgt_ty>,
                            ),
                    )*
                    $(
                        $conv_tgt_variant => return convert_vec::<$src_ty, $conv_tgt_ty>(
                            values,
                            validity,
                            ctx,
                            &span,
                            $conv_tgt_variant,
                            ColumnValues::push::<$conv_tgt_ty>,
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
            convert => []
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
            convert => [(Float4, f32), (Float8,f64), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int8, i64,
            promote => [(Int16, i128)],
            demote => [(Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint4, u32), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Int16, i128,
            promote => [],
            demote => [(Int8, i64), (Int4, i32), (Int2, i16), (Int1, i8)],
            convert => [(Float4, f32), (Float8,f64), (Uint8, u64), (Uint16, u128)]
        );

        adjust!(Uint1, u8,
            promote => [(Uint2, u16), (Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [],
            convert => [(Float4, f32), (Float8,f64), (Int1, i8), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint2, u16,
            promote => [(Uint4, u32), (Uint8, u64), (Uint16, u128)],
            demote => [(Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int2, i16), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint4, u32,
            promote => [(Uint8, u64), (Uint16, u128)],
            demote => [(Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int4, i32), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint8, u64,
            promote => [(Uint16, u128)],
            demote => [(Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int8, i64), (Int16, i128)]
        );

        adjust!(Uint16, u128,
            promote => [],
            demote => [(Uint8, u64), (Uint4, u32), (Uint2, u16), (Uint1, u8)],
            convert => [(Float4, f32), (Float8,f64), (Int16, i128)]
        );

        unimplemented!("{:?} -> {:?}", self.kind(), target)
    }
}

fn demote_vec<From, To>(
    values: &[From],
    validity: &[bool],
    demote: impl Demote,
    span: impl Fn() -> Span,
    target_kind: Kind,
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
    target_kind: Kind,
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
    target_kind: Kind,
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
        use reifydb_core::Kind;
        use reifydb_core::num::SafePromote;
        use reifydb_diagnostic::{IntoSpan, Span};

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
                Kind::Int2,
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
                Kind::Int2,
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
                Kind::Int2,
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
                Kind::Int2,
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
        use crate::frame::AsSlice;
        use crate::frame::column::adjust::demote_vec;
        use reifydb_core::Kind;
        use reifydb_core::num::SafeDemote;
        use reifydb_diagnostic::{IntoSpan, Span};

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
                Kind::Int1,
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
                Kind::Int1,
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
                Kind::Int1,
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
                Kind::Int1,
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
