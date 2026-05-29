// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
	value::{
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		value_type::ValueType,
	},
};

use crate::Result;

pub struct TemporalParser;

impl TemporalParser {
	pub fn from_temporal(fragment: Fragment, target: ValueType, row_count: usize) -> Result<ColumnBuffer> {
		Self::parse_temporal_type(fragment, target, row_count)
	}

	pub fn parse_temporal(fragment: Fragment, row_count: usize) -> Result<ColumnBuffer> {
		let value = fragment.text();

		if value.starts_with('P') || value.starts_with('p') {
			let duration = parse_duration(fragment.clone())?;
			Ok(ColumnBuffer::duration(vec![duration; row_count]))
		} else if value.contains(':') && value.contains('-') {
			let datetime = parse_datetime(fragment.clone())?;
			Ok(ColumnBuffer::datetime(vec![datetime; row_count]))
		} else if value.contains('-') {
			let date = parse_date(fragment.clone())?;
			Ok(ColumnBuffer::date(vec![date; row_count]))
		} else if value.contains(':') {
			let time = parse_time(fragment.clone())?;
			Ok(ColumnBuffer::time(vec![time; row_count]))
		} else {
			Err(TypeError::Temporal {
				kind: TemporalKind::UnrecognizedTemporalPattern,
				message: format!("Unrecognized temporal pattern: '{}'", fragment.text()),
				fragment,
			}
			.into())
		}
	}

	pub fn parse_temporal_type(fragment: Fragment, target: ValueType, row_count: usize) -> Result<ColumnBuffer> {
		match target {
			ValueType::Date => {
				let date = parse_date(fragment.clone())?;
				Ok(ColumnBuffer::date(vec![date; row_count]))
			}
			ValueType::DateTime => {
				let datetime = parse_datetime(fragment.clone())?;
				Ok(ColumnBuffer::datetime(vec![datetime; row_count]))
			}
			ValueType::Time => {
				let time = parse_time(fragment.clone())?;
				Ok(ColumnBuffer::time(vec![time; row_count]))
			}
			ValueType::Duration => {
				let duration = parse_duration(fragment.clone())?;
				Ok(ColumnBuffer::duration(vec![duration; row_count]))
			}
			_ => Err(TypeError::UnsupportedCast {
				from: ValueType::DateTime,
				to: target,
				fragment,
			}
			.into()),
		}
	}
}
