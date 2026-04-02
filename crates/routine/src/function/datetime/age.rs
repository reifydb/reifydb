// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, duration::Duration, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateTimeAge {
	info: FunctionInfo,
}

impl Default for DateTimeAge {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeAge {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("datetime::age"),
		}
	}
}

impl Function for DateTimeAge {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let col1 = &args[0];
		let col2 = &args[1];
		let (data1, bitvec1) = col1.data().unwrap_option();
		let (data2, bitvec2) = col2.data().unwrap_option();
		let row_count = data1.len();

		let result_data = match (data1, data2) {
			(ColumnData::DateTime(container1), ColumnData::DateTime(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(dt1), Some(dt2)) => {
							// Extract time nanos since midnight
							let nanos1 = dt1.time().to_nanos_since_midnight() as i64;
							let nanos2 = dt2.time().to_nanos_since_midnight() as i64;
							let mut nanos_diff = nanos1 - nanos2;
							let mut days_borrow: i32 = 0;

							if nanos_diff < 0 {
								days_borrow = 1;
								nanos_diff += 86_400_000_000_000;
							}

							// Extract date parts
							let date1 = dt1.date();
							let date2 = dt2.date();

							let y1 = date1.year();
							let m1 = date1.month() as i32;
							let day1 = date1.day() as i32;

							let y2 = date2.year();
							let m2 = date2.month() as i32;
							let day2 = date2.day() as i32;

							let mut years = y1 - y2;
							let mut months = m1 - m2;
							let mut days = day1 - day2 - days_borrow;

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
								days += Date::days_in_month(
									borrow_year,
									borrow_month as u32,
								) as i32;
							}

							if months < 0 {
								years -= 1;
								months += 12;
							}

							let total_months = years * 12 + months;
							container.push(Duration::new(total_months, days, nanos_diff)?);
						}
						_ => container.push_default(),
					}
				}

				ColumnData::Duration(container)
			}
			(ColumnData::DateTime(_), other) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (bitvec1, bitvec2) {
			(Some(bv), _) | (_, Some(bv)) => ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
