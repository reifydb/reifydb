// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::bytes::MaxBytes,
	container::decimal_array::decimals,
	value_type::ValueType,
};

use crate::function::support::column::utf8_column;

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
			if let Some(&value) = $container.values().get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
			} else {
				result_data.push(String::new());
			}
		}

		LargeStringArray::from(result_data)
	}};
}

#[macro_export]
macro_rules! process_float_column {
	($container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);

		for i in 0..$row_count {
			if let Some(&value) = $container.values().get(i) {
				result_data.push(format_bytes_internal(value as i64, $base, $units));
			} else {
				result_data.push(String::new());
			}
		}

		LargeStringArray::from(result_data)
	}};
}

#[macro_export]
macro_rules! process_decimal_column {
	($ctx:expr, $container:expr, $row_count:expr, $base:expr, $units:expr) => {{
		let mut result_data = Vec::with_capacity($row_count);

		for value in decimals($container) {
			let bytes = value.trunc().to_i128().and_then(|bytes| i64::try_from(bytes).ok()).ok_or_else(
				|| RoutineError::FunctionExecutionFailed {
					function: $ctx.fragment.clone(),
					reason: format!("{value} is out of range for {}", ValueType::Int8),
				},
			)?;
			result_data.push(format_bytes_internal(bytes, $base, $units));
		}

		LargeStringArray::from(result_data)
	}};
}

pub struct FormatBytes {
	info: RoutineInfo,
}

impl Default for FormatBytes {
	fn default() -> Self {
		Self::new()
	}
}

impl FormatBytes {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::format_bytes"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for FormatBytes {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::Int1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Int2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Int4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Int8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Uint1(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Uint2(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Uint4(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Uint8(container) => process_int_column!(container, row_count, 1024.0, &IEC_UNITS),
			ViewData::Float4(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ViewData::Float8(container) => {
				process_float_column!(container, row_count, 1024.0, &IEC_UNITS)
			}
			ViewData::Decimal(container) => {
				process_decimal_column!(ctx, container, row_count, 1024.0, &IEC_UNITS)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![
						ValueType::Int1,
						ValueType::Int2,
						ValueType::Int4,
						ValueType::Int8,
						ValueType::Uint1,
						ValueType::Uint2,
						ValueType::Uint4,
						ValueType::Uint8,
						ValueType::Float4,
						ValueType::Float8,
						ValueType::DECIMAL,
					],
					actual: data.get_type(),
				});
			}
		};

		Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, result_data))
	}
}

impl Function for FormatBytes {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}

pub(super) use process_decimal_column;
pub(super) use process_float_column;
pub(super) use process_int_column;
