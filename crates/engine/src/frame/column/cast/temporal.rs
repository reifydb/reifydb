// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::frame::ColumnValues;
use reifydb_core::diagnostic::cast;
use reifydb_core::value::temporal::{parse_date, parse_datetime, parse_interval, parse_time};
use reifydb_core::{BitVec, BorrowedSpan, Date, DateTime, Interval, OwnedSpan, Time, Type};

impl ColumnValues {
    pub(crate) fn to_temporal(
        &self,
        target: Type,
        span: impl Fn() -> OwnedSpan,
    ) -> crate::Result<ColumnValues> {
        if let ColumnValues::Utf8(values, bitvec) = self {
            match target {
                Type::Date => to_date(values, bitvec, span),
                Type::DateTime => to_datetime(values, bitvec, span),
                Type::Time => to_time(values, bitvec, span),
                Type::Interval => to_interval(values, bitvec, span),
                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }
    }
}

macro_rules! impl_to_temporal {
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
                        reifydb_core::Error(cast::invalid_temporal(proper_span, $target_type, e.0))
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
