// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Type,
	interface::fragment::Fragment,
	result::error::diagnostic::temporal,
	return_error,
	value::temporal::{
		parse_date, parse_datetime, parse_interval, parse_time,
	},
};

use crate::columnar::ColumnData;

pub struct TemporalParser;

impl TemporalParser {
	/// Parse temporal expression to a specific target type with detailed
	/// error handling
	pub fn from_temporal(
		fragment: impl Fragment,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Self::parse_temporal_type(fragment, target, row_count)
	}

	/// Parse a temporal constant expression and create a column with the
	/// specified row count
	pub fn parse_temporal(
		fragment: impl Fragment,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		let value = fragment.value();

		// Route based on character patterns
		if value.starts_with('P') || value.starts_with('p') {
			// Interval format (ISO 8601 duration)
			let interval = match parse_interval(fragment.clone()) {
				Ok(interval) => interval,
				Err(e) => return_error!(e.diagnostic()),
			};
			Ok(ColumnData::interval(vec![interval; row_count]))
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
			return_error!(temporal::unrecognized_temporal_pattern(
				fragment.clone()
			))
		}
	}

	/// Parse temporal to specific target type with detailed error handling
	pub fn parse_temporal_type(
		fragment: impl Fragment,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		use reifydb_core::result::error::diagnostic::cast;

		match target {
			Type::Date => {
				let date = match parse_date(fragment.clone()) {
					Ok(date) => date,
					Err(e) => return_error!(
						cast::invalid_temporal(
							fragment.clone(),
							Type::Date,
							e.0
						)
					),
				};
				Ok(ColumnData::date(vec![date; row_count]))
			}
			Type::DateTime => {
				let datetime =
					match parse_datetime(fragment.clone()) {
						Ok(datetime) => datetime,
						Err(e) => return_error!(
							cast::invalid_temporal(
								fragment.clone()
									.to_owned(
									),
								Type::DateTime,
								e.0
							)
						),
					};
				Ok(ColumnData::datetime(vec![
					datetime;
					row_count
				]))
			}
			Type::Time => {
				let time = match parse_time(fragment.clone()) {
					Ok(time) => time,
					Err(e) => return_error!(
						cast::invalid_temporal(
							fragment.clone(),
							Type::Time,
							e.0
						)
					),
				};
				Ok(ColumnData::time(vec![time; row_count]))
			}
			Type::Interval => {
				let interval =
					match parse_interval(fragment.clone()) {
						Ok(interval) => interval,
						Err(e) => return_error!(
							cast::invalid_temporal(
								fragment.clone()
									.to_owned(
									),
								Type::Interval,
								e.0
							)
						),
					};
				Ok(ColumnData::interval(vec![
					interval;
					row_count
				]))
			}
			_ => return_error!(cast::unsupported_cast(
				fragment.clone(),
				Type::DateTime,
				target
			)),
		}
	}
}
