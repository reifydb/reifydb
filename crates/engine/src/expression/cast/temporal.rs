// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
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

use crate::{Result, error::CastError};

pub fn to_temporal(data: &ColumnBuffer, target: Type, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	if let ColumnBuffer::Utf8 {
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
				let shape_type = data.get_type();
				Err(TypeError::UnsupportedCast {
					from: shape_type,
					to: target,
					fragment: lazy_fragment.fragment(),
				}
				.into())
			}
		}
	} else {
		let shape_type = data.get_type();
		Err(TypeError::UnsupportedCast {
			from: shape_type,
			to: target,
			fragment: lazy_fragment.fragment(),
		}
		.into())
	}
}

macro_rules! impl_to_temporal {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name(container: &Utf8Container, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
			let mut out = ColumnBuffer::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = container.get(idx).unwrap();

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
			Ok(out)
		}
	};
}

impl_to_temporal!(to_date, Date, Type::Date, parse_date);
impl_to_temporal!(to_datetime, DateTime, Type::DateTime, parse_datetime);
impl_to_temporal!(to_time, Time, Type::Time, parse_time);
impl_to_temporal!(to_duration, Duration, Type::Duration, parse_duration);
