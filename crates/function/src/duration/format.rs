// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct DurationFormat;

impl DurationFormat {
	pub fn new() -> Self {
		Self
	}
}

fn format_duration(months: i32, days: i32, nanos: i64, fmt: &str) -> Result<String, String> {
	let years = months / 12;
	let remaining_months = months % 12;

	let total_seconds = nanos / 1_000_000_000;
	let remaining_nanos = (nanos % 1_000_000_000).unsigned_abs();

	let hours = (total_seconds / 3600) % 24;
	let minutes = (total_seconds % 3600) / 60;
	let seconds = total_seconds % 60;

	let mut result = String::new();
	let mut chars = fmt.chars().peekable();

	while let Some(ch) = chars.next() {
		if ch == '%' {
			match chars.next() {
				Some('Y') => result.push_str(&format!("{}", years)),
				Some('M') => result.push_str(&format!("{}", remaining_months)),
				Some('D') => result.push_str(&format!("{}", days)),
				Some('h') => result.push_str(&format!("{}", hours)),
				Some('m') => result.push_str(&format!("{}", minutes)),
				Some('s') => result.push_str(&format!("{}", seconds)),
				Some('f') => result.push_str(&format!("{:09}", remaining_nanos)),
				Some('%') => result.push('%'),
				Some(c) => return Err(format!("invalid format specifier: '%{}'", c)),
				None => return Err("unexpected end of format string after '%'".to_string()),
			}
		} else {
			result.push(ch);
		}
	}

	Ok(result)
}

impl ScalarFunction for DurationFormat {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let dur_col = columns.get(0).unwrap();
		let fmt_col = columns.get(1).unwrap();

		match (dur_col.data(), fmt_col.data()) {
			(
				ColumnData::Duration(dur_container),
				ColumnData::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), fmt_container.is_defined(i)) {
						(Some(d), true) => {
							let fmt_str = &fmt_container[i];
							match format_duration(
								d.get_months(),
								d.get_days(),
								d.get_nanos(),
								fmt_str,
							) {
								Ok(formatted) => {
									result_data.push(formatted);
								}
								Err(reason) => {
									return Err(
										ScalarFunctionError::ExecutionFailed {
											function: ctx.fragment.clone(),
											reason,
										},
									);
								}
							}
						}
						_ => {
							result_data.push(String::new());
						}
					}
				}

				Ok(ColumnData::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				})
			}
			(ColumnData::Duration(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
