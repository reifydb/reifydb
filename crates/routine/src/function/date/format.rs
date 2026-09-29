// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::utf8;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{temporal_array::dates, varlen_array},
	date::Date,
	value_type::ValueType,
};

pub struct DateFormat {
	info: RoutineInfo,
}

impl Default for DateFormat {
	fn default() -> Self {
		Self::new()
	}
}

impl DateFormat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::format"),
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

fn compute_day_of_year(year: i32, month: u32, day: u32) -> u32 {
	let mut doy = 0u32;
	for m in 1..month {
		doy += Date::days_in_month(year, m);
	}
	doy + day
}

impl<'a> Routine<FunctionContext<'a>> for DateFormat {
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
		let date_data = ColumnView::try_from(&args[0])?;
		let fmt_data = ColumnView::try_from(&args[1])?;
		let row_count = date_data.len();

		let result_data = match (&date_data.data, &fmt_data.data) {
			(
				ViewData::Date(date_container),
				ViewData::Utf8 {
					container: fmt_container,
					..
				},
			) => {
				let mut result = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dates(date_container).get(i), varlen_array::get(fmt_container, i)) {
						(Some(d), Some(fmt_str)) => {
							let doy = compute_day_of_year(d.year(), d.month(), d.day());
							match format_date(d.year(), d.month(), d.day(), doy, fmt_str) {
								Ok(formatted) => {
									result.push(formatted);
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
							result.push(String::new());
						}
					}
				}

				utf8(ctx.fragment.text(), result)
			}
			(ViewData::Date(_), _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::Utf8],
					actual: fmt_data.get_type(),
				});
			}
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::Date],
					actual: date_data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateFormat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
