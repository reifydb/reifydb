// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::{Fragment, LazyFragment},
	value::{
		column_view::{ColumnView, ViewData},
		date::Date,
		datetime::DateTime,
		duration::Duration,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		time::Time,
		value_type::ValueType,
	},
};

use super::error::CastError;
use crate::value::column::builder::ColumnBuilder;

pub fn to_temporal(
	data: &ColumnView,
	target: ValueType,
	lazy_fragment: impl LazyFragment,
) -> Result<(FieldRef, ArrayRef)> {
	if let ViewData::Utf8 {
		container,
		..
	} = &data.data
	{
		let name = data.field.name();
		match target {
			ValueType::Date => to_date(container, name, lazy_fragment),
			ValueType::DateTime => to_datetime(container, name, lazy_fragment),
			ValueType::Time => to_time(container, name, lazy_fragment),
			ValueType::Duration => to_duration(container, name, lazy_fragment),
			_ => {
				let object_type = data.get_type();
				Err(TypeError::UnsupportedCast {
					from: object_type,
					to: target,
					fragment: lazy_fragment.fragment(),
				}
				.into())
			}
		}
	} else {
		let object_type = data.get_type();
		Err(TypeError::UnsupportedCast {
			from: object_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into())
	}
}

macro_rules! impl_to_temporal {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name(
			container: &LargeStringArray,
			name: &str,
			lazy_fragment: impl LazyFragment,
		) -> Result<(FieldRef, ArrayRef)> {
			let mut out = ColumnBuilder::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_valid(idx) {
					let val = container.value(idx);

					let temp_fragment = Fragment::internal(val);

					let parsed = $parse_fn(temp_fragment).map_err(|mut e| {
						let proper_fragment = lazy_fragment.fragment();

						if let Fragment::Internal {
							text: error_text,
						} = &e.0.fragment
						{
							if let Fragment::Statement {
								text: source_text,
								..
							} = &proper_fragment
							{
								if &**source_text == val
									|| source_text.contains(&format!("\"{}\"", val))
								{
									let offset =
										val.find(&**error_text).unwrap_or(0);
									e.0.fragment = proper_fragment
										.sub_fragment(offset, error_text.len());
								} else {
									e.0.fragment = proper_fragment.clone();
								}
							} else {
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
			Ok(out.finish(name))
		}
	};
}

impl_to_temporal!(to_date, Date, ValueType::Date, parse_date);
impl_to_temporal!(to_datetime, DateTime, ValueType::DateTime, parse_datetime);
impl_to_temporal!(to_time, Time, ValueType::Time, parse_time);
impl_to_temporal!(to_duration, Duration, ValueType::Duration, parse_duration);
