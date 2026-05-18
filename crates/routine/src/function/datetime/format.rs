// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, date::Date, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateTimeFormat {
	info: RoutineInfo,
}

impl Default for DateTimeFormat {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeFormat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::format"),
		}
	}
}

fn compute_day_of_year(year: i32, month: u32, day: u32) -> u32 {
	let mut doy = 0u32;
	for m in 1..month {
		doy += Date::days_in_month(year, m);
	}
	doy + day
}

#[allow(clippy::too_many_arguments)]
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

impl<'a> Routine<FunctionContext<'a>> for DateTimeFormat {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let dt_col = &args[0];
		let fmt_col = &args[1];
		let (dt_data, dt_bitvec) = dt_col.unwrap_option();
		let (fmt_data, fmt_bitvec) = fmt_col.unwrap_option();
		let row_count = dt_data.len();

		let result_data =
			match (dt_data, fmt_data) {
				(
					ColumnBuffer::DateTime(dt_container),
					ColumnBuffer::Utf8 {
						container: fmt_container,
						..
					},
				) => {
					let mut result = Vec::with_capacity(row_count);

					for i in 0..row_count {
						match (dt_container.get(i), fmt_container.is_defined(i)) {
							(Some(dt), true) => {
								let fmt_str = fmt_container.get(i).unwrap();
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
										result.push(formatted);
									}
									Err(reason) => {
										return Err(RoutineError::FunctionExecutionFailed {
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
				(ColumnBuffer::DateTime(_), other) => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
				(other, _) => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 0,
						expected: vec![Type::DateTime],
						actual: other.get_type(),
					});
				}
			};

		let final_data = match (dt_bitvec, fmt_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateTimeFormat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
