// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TimeFormat {
	info: FunctionInfo,
}

impl Default for TimeFormat {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeFormat {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("time::format"),
		}
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

impl Function for TimeFormat {
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

		let time_col = &args[0];
		let fmt_col = &args[1];

		let (time_data, time_bv) = time_col.unwrap_option();
		let (fmt_data, _) = fmt_col.unwrap_option();

		match (time_data, fmt_data) {
			(
				ColumnBuffer::Time(time_container),
				ColumnBuffer::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let row_count = time_data.len();
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
									return Err(FunctionError::ExecutionFailed {
										function: ctx.fragment.clone(),
										reason,
									});
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
				if let Some(bv) = time_bv {
					final_data = ColumnBuffer::Option {
						inner: Box::new(final_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			(ColumnBuffer::Time(_), other) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}
}
