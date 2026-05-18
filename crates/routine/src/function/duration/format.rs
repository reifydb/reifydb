// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DurationFormat {
	info: RoutineInfo,
}

impl Default for DurationFormat {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationFormat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::format"),
		}
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

impl<'a> Routine<FunctionContext<'a>> for DurationFormat {
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

		let dur_col = &args[0];
		let fmt_col = &args[1];

		let (dur_data, dur_bv) = dur_col.unwrap_option();
		let (fmt_data, _) = fmt_col.unwrap_option();

		match (dur_data, fmt_data) {
			(
				ColumnBuffer::Duration(dur_container),
				ColumnBuffer::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let row_count = dur_data.len();
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), fmt_container.is_defined(i)) {
						(Some(d), true) => {
							let fmt_str = fmt_container.get(i).unwrap();
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
										RoutineError::FunctionExecutionFailed {
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

				let mut final_data = ColumnBuffer::Utf8 {
					container: Utf8Container::new(result_data),
					max_bytes: MaxBytes::MAX,
				};
				if let Some(bv) = dur_bv {
					final_data = ColumnBuffer::Option {
						inner: Box::new(final_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			(ColumnBuffer::Duration(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for DurationFormat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
