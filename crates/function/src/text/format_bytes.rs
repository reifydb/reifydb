// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

const IEC_UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

pub(super) fn format_bytes_internal(bytes: i64, base: f64, units: &[&str]) -> String {
	if bytes == 0 {
		return "0 B".to_string();
	}

	let bytes_abs = bytes.unsigned_abs() as f64;
	let sign = if bytes < 0 {
		"-"
	} else {
		""
	};

	let mut unit_index = 0;
	let mut value = bytes_abs;

	while value >= base && unit_index < units.len() - 1 {
		value /= base;
		unit_index += 1;
	}

	if unit_index == 0 {
		format!("{}{} {}", sign, bytes_abs as i64, units[0])
	} else if value == value.floor() {
		format!("{}{} {}", sign, value as i64, units[unit_index])
	} else {
		let formatted = format!("{:.2}", value);
		let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
		format!("{}{} {}", sign, trimmed, units[unit_index])
	}
}

#[macro_export]
macro_rules! process_int_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(&value) = $container.get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
			} else {
				result_data.push(String::new());
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

#[macro_export]
macro_rules! process_float_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(&value) = $container.get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
			} else {
				result_data.push(String::new());
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

#[macro_export]
macro_rules! process_decimal_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(value) = $container.get(i) {
				// Truncate decimal to integer by parsing the integer part
				let s = value.to_string();
				let int_part = s.split('.').next().unwrap_or("0");
				let bytes = int_part.parse::<i64>().unwrap_or(0);
				result_data.push(format_bytes_internal(bytes, $base, $units));
			} else {
				result_data.push(String::new());
			}
		}

		Ok(ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		})
	}};
}

/// Formats bytes using binary units (1024-based: B, KiB, MiB, GiB, TiB, PiB)
pub struct FormatBytes;

impl FormatBytes {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for FormatBytes {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}

		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Int1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Int8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Uint8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ColumnData::Float4(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ColumnData::Float8(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				process_decimal_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			other => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
					Type::Float4,
					Type::Float8,
					Type::Decimal,
				],
				actual: other.get_type(),
			}),
		}
	}
}

pub(super) use process_decimal_column;
pub(super) use process_float_column;
pub(super) use process_int_column;
