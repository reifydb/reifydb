// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
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
	pub fn from_temporal(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnBuffer> {
		Self::parse_temporal_type(fragment, target, row_count)
	}

	/// Parse a temporal constant expression and create a column with the
	/// specified encoded count
	pub fn parse_temporal(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		let value = fragment.text();

		// Route based on character patterns
		if value.starts_with('P') || value.starts_with('p') {
			// Duration format (ISO 8601 duration)
			let duration = parse_duration(fragment.clone())?;
			Ok(ColumnBuffer::duration(vec![duration; row_count]))
		} else if value.contains(':') && value.contains('-') {
			// DateTime format (contains both : and -)
			let datetime = parse_datetime(fragment.clone())?;
			Ok(ColumnBuffer::datetime(vec![datetime; row_count]))
		} else if value.contains('-') {
			// Date format with - separators
			let date = parse_date(fragment.clone())?;
			Ok(ColumnBuffer::date(vec![date; row_count]))
		} else if value.contains(':') {
			// Time format (contains :)
			let time = parse_time(fragment.clone())?;
			Ok(ColumnBuffer::time(vec![time; row_count]))
		} else {
			// Unrecognized pattern
			Err(TypeError::Temporal {
				kind: TemporalKind::UnrecognizedTemporalPattern,
				message: format!("Unrecognized temporal pattern: '{}'", fragment.text()),
				fragment,
			}
			.into())
		}
	}

	/// Parse temporal to specific target type with detailed error handling
	pub fn parse_temporal_type(fragment: Fragment, target: Type, row_count: usize) -> Result<ColumnBuffer> {
		match target {
			Type::Date => {
				let date = parse_date(fragment.clone())?;
				Ok(ColumnBuffer::date(vec![date; row_count]))
			}
			Type::DateTime => {
				let datetime = parse_datetime(fragment.clone())?;
				Ok(ColumnBuffer::datetime(vec![datetime; row_count]))
			}
			Type::Time => {
				let time = parse_time(fragment.clone())?;
				Ok(ColumnBuffer::time(vec![time; row_count]))
			}
			Type::Duration => {
				let duration = parse_duration(fragment.clone())?;
				Ok(ColumnBuffer::duration(vec![duration; row_count]))
			}
			_ => Err(TypeError::UnsupportedCast {
				from: Type::DateTime,
				to: target,
				fragment,
			}
			.into()),
		}
	}
}
