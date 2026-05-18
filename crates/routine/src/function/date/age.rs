// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	error::TypeError,
	value::{container::temporal::TemporalContainer, date::Date, duration::Duration, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateAge {
	info: RoutineInfo,
}

impl Default for DateAge {
	fn default() -> Self {
		Self::new()
	}
}

impl DateAge {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::age"),
		}
	}
}

pub fn date_age(d1: &Date, d2: &Date) -> Result<Duration, Box<TypeError>> {
	let y1 = d1.year();
	let m1 = d1.month() as i32;
	let day1 = d1.day() as i32;

	let y2 = d2.year();
	let m2 = d2.month() as i32;
	let day2 = d2.day() as i32;

	let mut years = y1 - y2;
	let mut months = m1 - m2;
	let mut days = day1 - day2;

	if days < 0 {
		months -= 1;

		let borrow_month = if m1 - 1 < 1 {
			12
		} else {
			m1 - 1
		};
		let borrow_year = if m1 - 1 < 1 {
			y1 - 1
		} else {
			y1
		};
		days += Date::days_in_month(borrow_year, borrow_month as u32) as i32;
	}

	if months < 0 {
		years -= 1;
		months += 12;
	}

	let total_months = years * 12 + months;
	Duration::new(total_months, days, 0)
}

impl<'a> Routine<FunctionContext<'a>> for DateAge {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let col1 = &args[0];
		let col2 = &args[1];
		let (data1, bitvec1) = col1.unwrap_option();
		let (data2, bitvec2) = col2.unwrap_option();
		let row_count = data1.len();

		let result_data = match (data1, data2) {
			(ColumnBuffer::Date(container1), ColumnBuffer::Date(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(d1), Some(d2)) => {
							container.push(date_age(d1, d2)?);
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::Duration(container)
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (bitvec1, bitvec2) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateAge {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
