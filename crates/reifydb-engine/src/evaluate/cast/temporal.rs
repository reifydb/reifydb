// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use reifydb_core::{
	interface::fragment::{BorrowedFragment, OwnedFragment, Fragment, StatementLine, StatementColumn},
	Date, DateTime, Interval, Time, Type, error,
	result::error::diagnostic::cast,
	value::{
		container::StringContainer,
		temporal::{
			parse_date, parse_datetime, parse_interval, parse_time,
		},
	},
};

use crate::columnar::ColumnData;

pub fn to_temporal(
	data: &ColumnData,
	target: Type,
	fragment: impl Fn() -> OwnedFragment,
) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8(container) = data {
		match target {
			Type::Date => to_date(container, fragment),
			Type::DateTime => to_datetime(container, fragment),
			Type::Time => to_time(container, fragment),
			Type::Interval => to_interval(container, fragment),
			_ => {
				let source_type = data.get_type();
				reifydb_core::err!(cast::unsupported_cast(
					fragment(),
					source_type,
					target
				))
			}
		}
	} else {
		let source_type = data.get_type();
		reifydb_core::err!(cast::unsupported_cast(
			fragment(),
			source_type,
			target
		))
	}
}

macro_rules! impl_to_temporal {
    ($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
        #[inline]
        fn $fn_name(
            container: &StringContainer,
            fragment: impl Fn() -> OwnedFragment,
        ) -> crate::Result<ColumnData> {
            let mut out = ColumnData::with_capacity($target_type, container.len());
            for idx in 0..container.len() {
                if container.is_defined(idx) {
                    let val = &container[idx];
                    // Use internal fragment for now - the fragment will be replaced in error handling
                    let temp_fragment = BorrowedFragment::new_internal(val.as_str());

                    let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
                        // Get the original fragment for error reporting
                        let proper_fragment = fragment();


                        use reifydb_core::Fragment as _FragmentTrait;
                        let value_with_position = OwnedFragment::Statement {
                            text: val.to_string(),  // The actual value without quotes
                            line: proper_fragment.line(),
                            column: proper_fragment.column(),
                        };
                        e.0.with_fragment(value_with_position.clone());
                        
                        // Wrap in cast error with the original fragment
                        error!(cast::invalid_temporal(value_with_position, $target_type, e.0))
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
