// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

use crate::evaluate::{Convert, Demote, Promote};
use crate::frame::ColumnValues;
use reifydb_core::ValueKind;
use reifydb_core::ValueKind::{Int1, Int2};
use reifydb_core::num::{SafeConvert, SafeDemote, SafePromote};
use reifydb_diagnostic::Span;

impl ColumnValues {
    pub fn adjust_column(
        &self,
        target: ValueKind,
        context: impl Promote + Demote + Convert,
        span: impl Fn() -> Span,
    ) -> crate::Result<ColumnValues> {
        use ValueKind::*;

        if target == self.kind() {
            return Ok(self.clone());
        }

        if let ColumnValues::Int2(values, validity) = self {
            if target == Int1 {
                return demote_vec::<i16, i8>(
                    values,
                    validity,
                    context,
                    &span,
                    Int1,
                    ColumnValues::push::<i8>,
                );
            }
        }

        if let ColumnValues::Int1(values, validity) = self {
            if target == Int2 {
                return promote_vec::<i8, i16>(
                    values,
                    validity,
                    context,
                    &span,
                    Int2,
                    ColumnValues::push::<i16>,
                );
            }
            if target == Int4 {
                return promote_vec::<i8, i32>(
                    values,
                    validity,
                    context,
                    &span,
                    Int4,
                    ColumnValues::push::<i32>,
                );
            }
            if target == Int8 {
                return promote_vec::<i8, i64>(
                    values,
                    validity,
                    context,
                    &span,
                    Int8,
                    ColumnValues::push::<i64>,
                );
            }
            if target == Int16 {
                return promote_vec::<i8, i128>(
                    values,
                    validity,
                    context,
                    &span,
                    Int16,
                    ColumnValues::push::<i128>,
                );
            }

            if target == Uint1 {
                return convert_vec::<i8, u8>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint1,
                    ColumnValues::push::<u8>,
                );
            }

            if target == Uint2 {
                return convert_vec::<i8, u16>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint2,
                    ColumnValues::push::<u16>,
                );
            }

            if target == Uint4 {
                return convert_vec::<i8, u32>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint4,
                    ColumnValues::push::<u32>,
                );
            }

            if target == Uint8 {
                return convert_vec::<i8, u64>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint8,
                    ColumnValues::push::<u64>,
                );
            }

            if target == Uint16 {
                return convert_vec::<i8, u128>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint16,
                    ColumnValues::push::<u128>,
                );
            }
        }

        if let ColumnValues::Int2(values, validity) = self {
            if target == Int4 {
                return promote_vec::<i16, i32>(
                    values,
                    validity,
                    context,
                    &span,
                    Int4,
                    ColumnValues::push::<i32>,
                );
            }

            if target == Int16 {
                return promote_vec::<i16, i128>(
                    values,
                    validity,
                    context,
                    &span,
                    Int16,
                    ColumnValues::push::<i128>,
                );
            }

            if target == Uint2 {
                return convert_vec::<i16, u16>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint2,
                    ColumnValues::push::<u16>,
                );
            }

            if target == Uint4 {
                return convert_vec::<i16, u32>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint4,
                    ColumnValues::push::<u32>,
                );
            }

            if target == Uint16 {
                return convert_vec::<i16, u128>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint16,
                    ColumnValues::push::<u128>,
                );
            }
        }

        if let ColumnValues::Int4(values, validity) = self {
            if target == Int8 {
                return promote_vec::<i32, i64>(
                    values,
                    validity,
                    context,
                    &span,
                    Int8,
                    ColumnValues::push::<i64>,
                );
            }

            if target == Uint8 {
                return convert_vec::<i32, u64>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint8,
                    ColumnValues::push::<u64>,
                );
            }

            if target == Uint16 {
                return convert_vec::<i32, u128>(
                    values,
                    validity,
                    context,
                    &span,
                    Uint16,
                    ColumnValues::push::<u128>,
                );
            }
        }

        match self {
            ColumnValues::Int2(values, validity) if target == Int1 => {
                let mut out = ColumnValues::with_capacity(Int1, values.len());
                for (i, &val) in values.iter().enumerate() {
                    if validity[i] {
                        match context.demote::<i16, i8>(val, &span)? {
                            Some(v) => out.push::<i8>(v),
                            None => out.push_undefined(),
                        }
                    } else {
                        out.push_undefined();
                    }
                }
                Ok(out)
            }

            ColumnValues::Int4(values, validity) if target == Int2 => {
                let mut out = ColumnValues::with_capacity(Int2, values.len());
                for (i, &val) in values.iter().enumerate() {
                    if validity[i] {
                        match context.demote::<i32, i16>(val, &span)? {
                            Some(v) => out.push::<i16>(v),
                            None => out.push_undefined(),
                        }
                    } else {
                        out.push_undefined();
                    }
                }
                Ok(out)
            }
            _ => unimplemented!("{self:?} {target:?}"),
        }
    }
}

fn demote_vec<From, To>(
    values: &[From],
    validity: &[bool],
    demote: impl Demote,
    span: impl Fn() -> Span,
    target_kind: ValueKind,
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
    context: impl Promote,
    span: impl Fn() -> Span,
    target_kind: ValueKind,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafePromote<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            match context.promote::<From, To>(val, &span)? {
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
    context: impl Convert,
    span: impl Fn() -> Span,
    target_kind: ValueKind,
    mut push: impl FnMut(&mut ColumnValues, To),
) -> crate::Result<ColumnValues>
where
    From: Copy + SafeConvert<To>,
{
    let mut out = ColumnValues::with_capacity(target_kind, values.len());
    for (idx, &val) in values.iter().enumerate() {
        if validity[idx] {
            match context.convert::<From, To>(val, &span)? {
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
        use reifydb_core::ValueKind;
        use reifydb_core::num::SafePromote;
        use reifydb_diagnostic::IntoSpan;
        use reifydb_testing::make_test_span;

        #[test]
        fn test_ok() {
            let values = [1i8, 2i8];
            let validity = [true, true];
            let context = TestContext::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int2,
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
            let context = TestContext::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i8];
            let validity = [false];
            let context = TestContext::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_mixed_validity_and_promote_failure() {
            let values = [1i8, 42i8, 3i8, 4i8];
            let validity = [true, true, false, true];
            let context = TestContext::new();

            let result = promote_vec::<i8, i16>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int2,
                |col, v| col.push::<i16>(v),
            )
            .unwrap();

            let slice = result.as_slice::<i16>();
            assert_eq!(slice, &[1i16, 0, 0, 4i16]);
            assert_eq!(result.validity(), &[true, false, false, true]);
        }

        struct TestContext;

        impl TestContext {
            fn new() -> Self {
                Self
            }
        }

        impl Promote for &TestContext {
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
                Ok(Some(val.promote().unwrap()))
            }
        }
    }

    mod demote {
        use crate::evaluate::Demote;
        use crate::frame::AsSlice;
        use crate::frame::column::adjust::demote_vec;
        use reifydb_core::ValueKind;
        use reifydb_core::num::SafeDemote;
        use reifydb_diagnostic::IntoSpan;
        use reifydb_testing::make_test_span;

        #[test]
        fn test_ok() {
            let values = [1i16, 2i16];
            let validity = [true, true];
            let context = TestContext::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int1,
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
            let context = TestContext::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_invalid_bitmaps_are_undefined() {
            let values = [1i16];
            let validity = [false];
            let context = TestContext::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            assert_eq!(result.validity(), &[false]);
        }

        #[test]
        fn test_mixed_validity_and_demote_failure() {
            let values = [1i16, 42i16, 3i16, 4i16];
            let validity = [true, true, false, true];
            let context = TestContext::new();

            let result = demote_vec::<i16, i8>(
                &values,
                &validity,
                &context,
                || make_test_span(),
                ValueKind::Int1,
                |col, v| col.push::<i8>(v),
            )
            .unwrap();

            let slice: &[i8] = result.as_slice();
            assert_eq!(slice, &[1i8, 0, 0, 4i8]);
            assert_eq!(result.validity(), &[true, false, false, true]);
        }

        struct TestContext;

        impl TestContext {
            fn new() -> Self {
                Self
            }
        }

        impl Demote for &TestContext {
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
                Ok(Some(val.demote().unwrap()))
            }
        }
    }
}
