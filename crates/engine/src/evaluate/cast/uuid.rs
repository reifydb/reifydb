// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::error::diagnostic::cast;
use reifydb_core::frame::ColumnValues;
use reifydb_core::value::uuid::parse::{parse_uuid4, parse_uuid7};
use reifydb_core::value::uuid::{Uuid4, Uuid7};
use reifydb_core::{BitVec, BorrowedSpan, OwnedSpan, Type, error, CowVec};

pub fn to_uuid(
    values: &ColumnValues,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnValues> {
    match values {
        ColumnValues::Utf8(vals, bitvec) => from_text(vals, bitvec, target, span),
        ColumnValues::Uuid4(vals, bitvec) => from_uuid4(vals, bitvec, target, span),
        ColumnValues::Uuid7(vals, bitvec) => from_uuid7(vals, bitvec, target, span),
        _ => {
            let source_type = values.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

#[inline]
fn from_text(
    values: &[String],
    bitvec: &BitVec,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnValues> {
    match target {
        Type::Uuid4 => to_uuid4(values, bitvec, span),
        Type::Uuid7 => to_uuid7(values, bitvec, span),
        _ => {
            let source_type = Type::Utf8;
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

macro_rules! impl_to_uuid {
    ($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
        #[inline]
        fn $fn_name(
            values: &[String],
            bitvec: &BitVec,
            span: impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnValues> {
            let mut out = ColumnValues::with_capacity($target_type, values.len());
            for (idx, val) in values.iter().enumerate() {
                if bitvec.get(idx) {
                    let temp_span = BorrowedSpan::new(val.as_str());

                    let parsed = $parse_fn(temp_span).map_err(|mut e| {
                        // Only create proper span on error
                        let proper_span = span();

                        // Update the diagnostic span
                        if let Some(ref mut diagnostic_span) = e.0.span {
                            *diagnostic_span = proper_span.clone();
                        }

                        e.0.update_spans(&proper_span);
                        error!(cast::invalid_uuid(proper_span, $target_type, e.0))
                    })?;

                    out.push::<$type>(parsed);
                } else {
                    out.push_undefined();
                }
            }
            Ok(out)
        }
    };
}

impl_to_uuid!(to_uuid4, Uuid4, Type::Uuid4, parse_uuid4);
impl_to_uuid!(to_uuid7, Uuid7, Type::Uuid7, parse_uuid7);

#[inline]
fn from_uuid4(values: &[Uuid4], bitvec: &BitVec, target: Type, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match target {
        Type::Uuid4 => {
            // Same type, just clone
            Ok(ColumnValues::Uuid4(CowVec::new(values.to_vec()), bitvec.clone()))
        }
        _ => {
            // UUID4 to other types should be handled by the main cast routing
            // This allows UUID4 to be cast to text, etc.
            let source_type = Type::Uuid4;
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

#[inline]
fn from_uuid7(values: &[Uuid7], bitvec: &BitVec, target: Type, span: impl Fn() -> OwnedSpan) -> crate::Result<ColumnValues> {
    match target {
        Type::Uuid7 => {
            // Same type, just clone  
            Ok(ColumnValues::Uuid7(CowVec::new(values.to_vec()), bitvec.clone()))
        }
        _ => {
            // UUID7 to other types should be handled by the main cast routing
            // This allows UUID7 to be cast to text, etc.
            let source_type = Type::Uuid7;
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}
