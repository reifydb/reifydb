// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::columnar::ColumnData;
use reifydb_core::result::error::diagnostic::cast;
use reifydb_core::value::container::StringContainer;
use reifydb_core::value::temporal::{parse_date, parse_datetime, parse_interval, parse_time};
use reifydb_core::{BorrowedSpan, Date, DateTime, Interval, OwnedSpan, Time, Type, error};

pub fn to_temporal(
    data: &ColumnData,
    target: Type,
    span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
    if let ColumnData::Utf8(container) = data {
        match target {
            Type::Date => to_date(container, span),
            Type::DateTime => to_datetime(container, span),
            Type::Time => to_time(container, span),
            Type::Interval => to_interval(container, span),
            _ => {
                let source_type = data.get_type();
                reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
            }
        }
    } else {
        let source_type = data.get_type();
        reifydb_core::err!(cast::unsupported_cast(span(), source_type, target))
    }
}

macro_rules! impl_to_temporal {
    ($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
        #[inline]
        fn $fn_name(
            container: &StringContainer,
            span: impl Fn() -> OwnedSpan,
        ) -> crate::Result<ColumnData> {
            let mut out = ColumnData::with_capacity($target_type, container.len());
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
                        error!(cast::invalid_temporal(proper_span, $target_type, e.0))
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

impl_to_temporal!(to_date, Date, Type::Date, parse_date);
impl_to_temporal!(to_datetime, DateTime, Type::DateTime, parse_datetime);
impl_to_temporal!(to_time, Time, Type::Time, parse_time);
impl_to_temporal!(to_interval, Interval, Type::Interval, parse_interval);
