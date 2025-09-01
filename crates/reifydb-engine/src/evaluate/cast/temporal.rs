// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	Date, DateTime, Interval, Time, Type, error,
    value::{
		container::StringContainer,
    },
};
use reifydb_type::::{BorrowedFragment, LazyFragment, OwnedFragment};
use reifydb_type::::diagnostic::cast;
use reifydb_type::{
    parse_date, parse_datetime, parse_interval, parse_time,
};
use crate::columnar::ColumnData;

pub fn to_temporal<'a>(
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8(container) = data {
		match target {
			Type::Date => to_date(container, lazy_fragment),
			Type::DateTime => to_datetime(container, lazy_fragment),
			Type::Time => to_time(container, lazy_fragment),
			Type::Interval => to_interval(container, lazy_fragment),
			_ => {
				let source_type = data.get_type();
				reifydb_core::err!(cast::unsupported_cast(
					lazy_fragment.fragment(),
					source_type,
					target
				))
			}
		}
	} else {
		let source_type = data.get_type();
		reifydb_core::err!(cast::unsupported_cast(
			lazy_fragment.fragment(),
			source_type,
			target
		))
	}
}

macro_rules! impl_to_temporal {
    ($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
        #[inline]
        fn $fn_name<'a>(
            container: &StringContainer,
            lazy_fragment: impl LazyFragment<'a>,
        ) -> crate::Result<ColumnData> {
            let mut out = ColumnData::with_capacity($target_type, container.len());
            for idx in 0..container.len() {
                if container.is_defined(idx) {
                    let val = &container[idx];
                    // Use internal fragment for parsing - positions will be replaced with actual source positions
                    let temp_fragment = BorrowedFragment::new_internal(val.as_str());

                    let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
                        // Get the original fragment for error reporting
                        let proper_fragment = lazy_fragment.fragment().into_owned();

                        // Handle fragment replacement based on the context
                        // For Internal fragments (from parsing), we need to adjust position
                        if let OwnedFragment::Internal { text: error_text } = &e.0.fragment {
                            // Check if we're dealing with a string literal (Statement fragment)
                            // that contains position information we can use for sub-fragments
                            if let OwnedFragment::Statement { text: source_text, .. } = &proper_fragment {
                                // For string literals, if the source text exactly matches the value being parsed,
                                // or contains it with quotes, it's a string literal
                                if source_text == val.as_str() || source_text.contains(&format!("\"{}\"", val.as_str())) {
                                    // This is a string literal - adjust position within the string
                                    let offset = val.as_str().find(error_text.as_str()).unwrap_or(0);
                                    e.0.fragment = proper_fragment.sub_fragment(offset, error_text.len());
                                } else {
                                    // This is a column reference - use the column name
                                    e.0.fragment = proper_fragment.clone();
                                }
                            } else {
                                // Not a Statement fragment - use as is (for column references)
                                e.0.fragment = proper_fragment.clone();
                            }
                        }

                        // Wrap in cast error with the original fragment for the outer error
                        error!(cast::invalid_temporal(proper_fragment, $target_type, e.0))
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
