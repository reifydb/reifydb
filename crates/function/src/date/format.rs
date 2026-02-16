// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateFormat;

impl DateFormat {
	pub fn new() -> Self {
		Self
	}
}

fn format_date(year: i32, month: u32, day: u32, day_of_year: u32, fmt: &str) -> Result<String, String> {
	let mut result = String::new();
	let mut chars = fmt.chars().peekable();

	while let Some(ch) = chars.next() {
		if ch == '%' {
			match chars.next() {
				Some('Y') => result.push_str(&format!("{:04}", year)),
				Some('m') => result.push_str(&format!("{:02}", month)),
				Some('d') => result.push_str(&format!("{:02}", day)),
				Some('j') => result.push_str(&format!("{:03}", day_of_year)),
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

/// Compute day of year from year/month/day
fn compute_day_of_year(year: i32, month: u32, day: u32) -> u32 {
	use reifydb_type::value::date::Date;
	let mut doy = 0u32;
	for m in 1..month {
		doy += Date::days_in_month(year, m);
	}
	doy + day
}

impl ScalarFunction for DateFormat {
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

		let date_col = columns.get(0).unwrap();
		let fmt_col = columns.get(1).unwrap();

		match (date_col.data(), fmt_col.data()) {
			(
				ColumnData::Date(date_container),
				ColumnData::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), fmt_container.is_defined(i)) {
						(Some(d), true) => {
							let fmt_str = &fmt_container[i];
							let doy = compute_day_of_year(d.year(), d.month(), d.day());
							match format_date(d.year(), d.month(), d.day(), doy, fmt_str) {
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
			(ColumnData::Date(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}
}
