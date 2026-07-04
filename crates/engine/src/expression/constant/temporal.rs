// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::{
	error::{TemporalKind, TypeError},
	fragment::Fragment,
	value::temporal::parse::{
		date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
	},
};

use crate::Result;

pub struct TemporalParser;

impl TemporalParser {
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
}
