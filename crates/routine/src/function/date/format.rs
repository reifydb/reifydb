// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, date::Date, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateFormat {
	info: FunctionInfo,
}

impl Default for DateFormat {
	fn default() -> Self {
		Self::new()
	}
}

impl DateFormat {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("date::format"),
		}
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
	let mut doy = 0u32;
	for m in 1..month {
		doy += Date::days_in_month(year, m);
	}
	doy + day
}

impl Function for DateFormat {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let date_col = &args[0];
		let fmt_col = &args[1];
		let (date_data, date_bitvec) = date_col.unwrap_option();
		let (fmt_data, fmt_bitvec) = fmt_col.unwrap_option();
		let row_count = date_data.len();

		let result_data = match (date_data, fmt_data) {
			(
				ColumnBuffer::Date(date_container),
				ColumnBuffer::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), fmt_container.is_defined(i)) {
						(Some(d), true) => {
							let fmt_str = &fmt_container[i];
							let doy = compute_day_of_year(d.year(), d.month(), d.day());
							match format_date(d.year(), d.month(), d.day(), doy, fmt_str) {
								Ok(formatted) => {
									result.push(formatted);
								}
								Err(reason) => {
									return Err(FunctionError::ExecutionFailed {
										function: ctx.fragment.clone(),
										reason,
									});
								}
							}
						}
						_ => {
							result.push(String::new());
						}
					}
				}

				ColumnBuffer::Utf8 {
					container: Utf8Container::new(result),
					max_bytes: MaxBytes::MAX,
				}
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Utf8],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (date_bitvec, fmt_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
