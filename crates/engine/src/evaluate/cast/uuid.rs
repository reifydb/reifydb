// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::column::EngineColumnData;
use crate::column::container::{StringContainer, UuidContainer};
use reifydb_core::error::diagnostic::cast;
use reifydb_core::value::uuid::parse::{parse_uuid4, parse_uuid7};
use reifydb_core::value::uuid::{Uuid4, Uuid7};
use reifydb_core::{BorrowedSpan, OwnedSpan, Type, error};

pub fn to_uuid(
    data: &EngineColumnData,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<EngineColumnData> {
    match data {
        EngineColumnData::Utf8(container) => from_text(container, target, span),
        EngineColumnData::Uuid4(container) => from_uuid4(container, target, span),
        EngineColumnData::Uuid7(container) => from_uuid7(container, target, span),
        _ => {
            let source_type = data.get_type();
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}

#[inline]
fn from_text(
    container: &StringContainer,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<EngineColumnData> {
    match target {
        Type::Uuid4 => to_uuid4(container, span),
        Type::Uuid7 => to_uuid7(container, span),
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
            container: &StringContainer,
            span: impl Fn() -> OwnedSpan,
        ) -> crate::Result<EngineColumnData> {
            let mut out = EngineColumnData::with_capacity($target_type, container.len());
            for idx in 0..container.len() {
                if container.is_defined(idx) {
                    let val = &container[idx];
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
fn from_uuid4(
    container: &UuidContainer<Uuid4>,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<EngineColumnData> {
    match target {
        Type::Uuid4 => {
            // Same type, just clone
            Ok(EngineColumnData::Uuid4(UuidContainer::new(
                container.data().to_vec(),
                container.bitvec().clone(),
            )))
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
fn from_uuid7(
    container: &UuidContainer<Uuid7>,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<EngineColumnData> {
    match target {
        Type::Uuid7 => {
            // Same type, just clone
            Ok(EngineColumnData::Uuid7(UuidContainer::new(
                container.data().to_vec(),
                container.bitvec().clone(),
            )))
        }
        _ => {
            // UUID7 to other types should be handled by the main cast routing
            // This allows UUID7 to be cast to text, etc.
            let source_type = Type::Uuid7;
            reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
        }
    }
}
