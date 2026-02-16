// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, date::Date, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTimeFormat;

impl DateTimeFormat {
	pub fn new() -> Self {
		Self
	}
}

/// Compute day of year from year/month/day
fn compute_day_of_year(year: i32, month: u32, day: u32) -> u32 {
	let mut doy = 0u32;
	for m in 1..month {
		doy += Date::days_in_month(year, m);
	}
	doy + day
}

fn format_datetime(
	year: i32,
	month: u32,
	day: u32,
	hour: u32,
	minute: u32,
	second: u32,
	nanosecond: u32,
	fmt: &str,
) -> Result<String, String> {
	let mut result = String::new();
	let mut chars = fmt.chars().peekable();

	while let Some(ch) = chars.next() {
		if ch == '%' {
			match chars.peek() {
				Some('Y') => {
					chars.next();
					result.push_str(&format!("{:04}", year));
				}
				Some('m') => {
					chars.next();
					result.push_str(&format!("{:02}", month));
				}
				Some('d') => {
					chars.next();
					result.push_str(&format!("{:02}", day));
				}
				Some('j') => {
					chars.next();
					let doy = compute_day_of_year(year, month, day);
					result.push_str(&format!("{:03}", doy));
				}
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

impl ScalarFunction for DateTimeFormat {
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

		let dt_col = columns.get(0).unwrap();
		let fmt_col = columns.get(1).unwrap();

		match (dt_col.data(), fmt_col.data()) {
			(
				ColumnData::DateTime(dt_container),
				ColumnData::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), fmt_container.is_defined(i)) {
						(Some(dt), true) => {
							let fmt_str = &fmt_container[i];
							match format_datetime(
								dt.year(),
								dt.month(),
								dt.day(),
								dt.hour(),
								dt.minute(),
								dt.second(),
								dt.nanosecond(),
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
			(ColumnData::DateTime(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::DateTime],
				actual: other.get_type(),
			}),
		}
	}
}
