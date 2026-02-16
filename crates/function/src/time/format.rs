// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct TimeFormat;

impl TimeFormat {
	pub fn new() -> Self {
		Self
	}
}

fn format_time(hour: u32, minute: u32, second: u32, nanosecond: u32, fmt: &str) -> Result<String, String> {
	let mut result = String::new();
	let mut chars = fmt.chars().peekable();

	while let Some(ch) = chars.next() {
		if ch == '%' {
			match chars.peek() {
				Some('H') => {
					chars.next();
					result.push_str(&format!("{:02}", hour));
				}
				Some('M') => {
					chars.next();
					result.push_str(&format!("{:02}", minute));
				}
				Some('S') => {
					chars.next();
					result.push_str(&format!("{:02}", second));
				}
				Some('f') => {
					chars.next();
					result.push_str(&format!("{:09}", nanosecond));
				}
				Some('3') => {
					chars.next();
					if chars.peek() == Some(&'f') {
						chars.next();
						result.push_str(&format!("{:03}", nanosecond / 1_000_000));
					} else {
						return Err(
							"invalid format specifier: '%3' (expected '%3f')".to_string()
						);
					}
				}
				Some('6') => {
					chars.next();
					if chars.peek() == Some(&'f') {
						chars.next();
						result.push_str(&format!("{:06}", nanosecond / 1_000));
					} else {
						return Err(
							"invalid format specifier: '%6' (expected '%6f')".to_string()
						);
					}
				}
				Some('%') => {
					chars.next();
					result.push('%');
				}
				Some(c) => {
					let c = *c;
					return Err(format!("invalid format specifier: '%{}'", c));
				}
				None => return Err("unexpected end of format string after '%'".to_string()),
			}
		} else {
			result.push(ch);
		}
	}

	Ok(result)
}

impl ScalarFunction for TimeFormat {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
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

		let time_col = columns.get(0).unwrap();
		let fmt_col = columns.get(1).unwrap();

		match (time_col.data(), fmt_col.data()) {
			(
				ColumnData::Time(time_container),
				ColumnData::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (time_container.get(i), fmt_container.is_defined(i)) {
						(Some(t), true) => {
							let fmt_str = &fmt_container[i];
							match format_time(
								t.hour(),
								t.minute(),
								t.second(),
								t.nanosecond(),
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
			(ColumnData::Time(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
