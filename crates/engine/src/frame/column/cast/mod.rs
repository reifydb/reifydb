// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;

use crate::evaluate::{Convert, Demote, Promote};
use crate::frame::ColumnValues;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};
use reifydb_core::{BitVec, GetType, Span, Type};

impl ColumnValues {
    pub fn cast(
        &self,
        target: Type,
        ctx: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        match target {
            _ if target == self.get_type() => Ok(self.clone()),
            _ if target.is_number() => self.to_number(target, ctx, span),
            _ if target.is_bool() => self.to_boolean(span),
            _ if target.is_utf8() => self.to_text(),
            _ if target.is_temporal() => self.to_temporal(target, span),
            _ => unreachable!(),
        }
    }
}

pub(crate) fn demote_vec<From, To>(
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

pub(crate) fn promote_vec<From, To>(
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

pub(crate) fn convert_vec<From, To>(
    values: &[From],
    bitvec: &BitVec,
    ctx: impl Convert,
    span: impl Fn() -> Span,
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
        use crate::frame::column::cast::promote_vec;
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
        use crate::frame::column::cast::demote_vec;
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
