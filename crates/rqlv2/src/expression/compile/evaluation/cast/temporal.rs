// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast to temporal types (Date, DateTime, Time, Duration)

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	fragment::Fragment,
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

use crate::expression::types::{EvalError, EvalResult};

pub(super) fn to_temporal(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	if let ColumnData::Utf8 {
		container,
		..
	} = data
	{
		match target {
			Type::Date => to_date(container),
			Type::DateTime => to_datetime(container),
			Type::Time => to_time(container),
			Type::Duration => to_duration(container),
			_ => {
				let source_type = data.get_type();
				Err(EvalError::UnsupportedCast {
					from: format!("{:?}", source_type),
					to: format!("{:?}", target),
				})
			}
		}
	} else {
		let source_type = data.get_type();
		Err(EvalError::UnsupportedCast {
			from: format!("{:?}", source_type),
			to: format!("{:?}", target),
		})
	}
}

macro_rules! impl_to_temporal {
	($fn_name:ident, $type:ty, $target_type:expr, $parse_fn:expr) => {
		#[inline]
		fn $fn_name(container: &Utf8Container) -> EvalResult<ColumnData> {
			let mut out = ColumnData::with_capacity($target_type, container.len());
			for idx in 0..container.len() {
				if container.is_defined(idx) {
					let val = &container[idx];
					// Use internal fragment for parsing
					let temp_fragment = Fragment::internal(val.as_str());

					let parsed = $parse_fn(temp_fragment).map_err(|_e| EvalError::InvalidCast {
						details: format!("Cannot parse '{}' as {:?}", val, $target_type),
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
impl_to_temporal!(to_duration, Duration, Type::Duration, parse_duration);
