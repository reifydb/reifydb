// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::diagnostic::temporal,
	fragment::Fragment,
	return_error,
	value::{
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		r#type::Type,
	},
};

pub struct TemporalParser;

impl TemporalParser {
	/// Parse temporal expression to a specific target type with detailed
	/// error handling
	pub fn from_temporal<'a>(fragment: Fragment, target: Type, row_count: usize) -> crate::Result<ColumnData> {
		Self::parse_temporal_type(fragment, target, row_count)
	}

	/// Parse a temporal constant expression and create a column with the
	/// specified encoded count
	pub fn parse_temporal<'a>(fragment: Fragment, row_count: usize) -> crate::Result<ColumnData> {
		let value = fragment.text();

		// Route based on character patterns
		if value.starts_with('P') || value.starts_with('p') {
			// Duration format (ISO 8601 duration)
			let duration = match parse_duration(fragment.clone()) {
				Ok(duration) => duration,
				Err(e) => return_error!(e.diagnostic()),
			};
			Ok(ColumnData::duration(vec![duration; row_count]))
		} else if value.contains(':') && value.contains('-') {
			// DateTime format (contains both : and -)
			let datetime = match parse_datetime(fragment.clone()) {
				Ok(datetime) => datetime,
				Err(e) => return_error!(e.diagnostic()),
			};
			Ok(ColumnData::datetime(vec![datetime; row_count]))
		} else if value.contains('-') {
			// Date format with - separators
			let date = match parse_date(fragment.clone()) {
				Ok(date) => date,
				Err(e) => return_error!(e.diagnostic()),
			};
			Ok(ColumnData::date(vec![date; row_count]))
		} else if value.contains(':') {
			// Time format (contains :)
			let time = match parse_time(fragment.clone()) {
				Ok(time) => time,
				Err(e) => return_error!(e.diagnostic()),
			};
			Ok(ColumnData::time(vec![time; row_count]))
		} else {
			// Unrecognized pattern
			return_error!(temporal::unrecognized_temporal_pattern(fragment))
		}
	}

	/// Parse temporal to specific target type with detailed error handling
	pub fn parse_temporal_type<'a>(
		fragment: Fragment,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		use reifydb_type::error::diagnostic::cast;
		match target {
			Type::Date => {
				let date = match parse_date(fragment.clone()) {
					Ok(date) => date,
					Err(e) => return_error!(cast::invalid_temporal(fragment, Type::Date, e.0)),
				};
				Ok(ColumnData::date(vec![date; row_count]))
			}
			Type::DateTime => {
				let datetime = match parse_datetime(fragment.clone()) {
					Ok(datetime) => datetime,
					Err(e) => return_error!(cast::invalid_temporal(fragment, Type::DateTime, e.0)),
				};
				Ok(ColumnData::datetime(vec![datetime; row_count]))
			}
			Type::Time => {
				let time = match parse_time(fragment.clone()) {
					Ok(time) => time,
					Err(e) => return_error!(cast::invalid_temporal(fragment, Type::Time, e.0)),
				};
				Ok(ColumnData::time(vec![time; row_count]))
			}
			Type::Duration => {
				let duration = match parse_duration(fragment.clone()) {
					Ok(duration) => duration,
					Err(e) => return_error!(cast::invalid_temporal(fragment, Type::Duration, e.0)),
				};
				Ok(ColumnData::duration(vec![duration; row_count]))
			}
			_ => return_error!(cast::unsupported_cast(fragment, Type::DateTime, target)),
		}
	}
}
