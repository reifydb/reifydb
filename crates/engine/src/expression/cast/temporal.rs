// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		container::utf8::Utf8Container,
		date::Date,
		datetime::DateTime,
		duration::Duration,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		time::Time,
		r#type::Type,
	},
};

use crate::error::CastError;

pub fn to_temporal(data: &ColumnData, target: Type, lazy_fragment: impl LazyFragment) -> crate::Result<ColumnData> {
	if let ColumnData::Utf8 {
		container,
		..
	} = data
	{
		match target {
			Type::Date => to_date(container, lazy_fragment),
			Type::DateTime => to_datetime(container, lazy_fragment),
			Type::Time => to_time(container, lazy_fragment),
			Type::Duration => to_duration(container, lazy_fragment),
			_ => {
				let source_type = data.get_type();
				Err(TypeError::UnsupportedCast {
					from: source_type,
					to: target,
					fragment: lazy_fragment.fragment(),
				}
				.into())
			}
		}
	} else {
		let source_type = data.get_type();
		Err(TypeError::UnsupportedCast {
			from: source_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into())
	}
}

macro_rules! impl_to_temporal {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name<'a>(
			container: &Utf8Container,
			lazy_fragment: impl LazyFragment,
		) -> crate::Result<ColumnData> {
			let mut out = ColumnData::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					// Use internal fragment for parsing - positions will be replaced with actual
					// source positions
					let temp_fragment = Fragment::internal(val.as_str());

					let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
						// Get the original fragment for error reporting
						let proper_fragment = lazy_fragment.fragment();

						// Handle fragment replacement based on the context
						// For Internal fragments (from parsing), we need to adjust position
						if let Fragment::Internal {
							text: error_text,
						} = &e.0.fragment
						{
							// Check if we're dealing with a string literal (Statement
							// fragment) that contains position information we can use
							// for sub-fragments
							if let Fragment::Statement {
								text: source_text,
								..
							} = &proper_fragment
							{
								// For string literals, if the source text exactly
								// matches the value being parsed, or contains it
								// with quotes, it's a string literal
								if &**source_text == val.as_str()
									|| source_text.contains(&format!(
										"\"{}\"",
										val.as_str()
									)) {
									// This is a string literal - adjust position
									// within the string
									let offset = val
										.as_str()
										.find(&**error_text)
										.unwrap_or(0);
									e.0.fragment = proper_fragment
										.sub_fragment(offset, error_text.len());
								} else {
									// This is a column reference - use the column
									// name
									e.0.fragment = proper_fragment.clone();
								}
							} else {
								// Not a Statement fragment - use as is (for column
								// references)
								e.0.fragment = proper_fragment.clone();
							}
						}

						Error::from(CastError::InvalidTemporal {
							fragment: e.0.fragment.clone(),
							target: $target_type,
							cause: e.diagnostic(),
						})
					})?;

					out.push::<$type>(parsed);
				} else {
					out.push_none();
				}
			}
			Ok(out)
		}
	};
}

impl_to_temporal!(to_date, Date, Type::Date, parse_date);
impl_to_temporal!(to_datetime, DateTime, Type::DateTime, parse_datetime);
impl_to_temporal!(to_time, Time, Type::Time, parse_time);
impl_to_temporal!(to_duration, Duration, Type::Duration, parse_duration);
