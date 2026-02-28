// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
	value::{
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		r#type::Type,
	},
};

use crate::Result;

pub struct TemporalParser;

impl TemporalParser {
	/// Parse temporal expression to a specific target type with detailed
	/// error handling
	pub fn from_temporal<'a>(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnData> {
		Self::parse_temporal_type(fragment, target, row_count)
	}

	/// Parse a temporal constant expression and create a column with the
	/// specified encoded count
	pub fn parse_temporal<'a>(fragment: Fragment, row_count: usize) -> Result<ColumnData> {
		let value = fragment.text();

		// Route based on character patterns
		if value.starts_with('P') || value.starts_with('p') {
			// Duration format (ISO 8601 duration)
			let duration = parse_duration(fragment.clone()).map_err(|e| e)?;
			Ok(ColumnData::duration(vec![duration; row_count]))
		} else if value.contains(':') && value.contains('-') {
			// DateTime format (contains both : and -)
			let datetime = parse_datetime(fragment.clone()).map_err(|e| e)?;
			Ok(ColumnData::datetime(vec![datetime; row_count]))
		} else if value.contains('-') {
			// Date format with - separators
			let date = parse_date(fragment.clone()).map_err(|e| e)?;
			Ok(ColumnData::date(vec![date; row_count]))
		} else if value.contains(':') {
			// Time format (contains :)
			let time = parse_time(fragment.clone()).map_err(|e| e)?;
			Ok(ColumnData::time(vec![time; row_count]))
		} else {
			// Unrecognized pattern
			return Err(TypeError::Temporal {
				kind: TemporalKind::UnrecognizedTemporalPattern,
				message: format!("Unrecognized temporal pattern: '{}'", fragment.text()),
				fragment,
			}
			.into());
		}
	}

	/// Parse temporal to specific target type with detailed error handling
	pub fn parse_temporal_type<'a>(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnData> {
		match target {
			Type::Date => {
				let date = parse_date(fragment.clone()).map_err(|e| e)?;
				Ok(ColumnData::date(vec![date; row_count]))
			}
			Type::DateTime => {
				let datetime = parse_datetime(fragment.clone()).map_err(|e| e)?;
				Ok(ColumnData::datetime(vec![datetime; row_count]))
			}
			Type::Time => {
				let time = parse_time(fragment.clone()).map_err(|e| e)?;
				Ok(ColumnData::time(vec![time; row_count]))
			}
			Type::Duration => {
				let duration = parse_duration(fragment.clone()).map_err(|e| e)?;
				Ok(ColumnData::duration(vec![duration; row_count]))
			}
			_ => {
				return Err(TypeError::UnsupportedCast {
					from: Type::DateTime,
					to: target,
					fragment,
				}
				.into());
			}
		}
	}
}
