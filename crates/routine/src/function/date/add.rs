// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateAdd {
	info: FunctionInfo,
}

impl Default for DateAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl DateAdd {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("date::add"),
		}
	}
}

impl Function for DateAdd {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
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
		let dur_col = &args[1];
		let (date_data, date_bitvec) = date_col.unwrap_option();
		let (dur_data, dur_bitvec) = dur_col.unwrap_option();
		let row_count = date_data.len();

		let result_data = match (date_data, dur_data) {
			(ColumnBuffer::Date(date_container), ColumnBuffer::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), dur_container.get(i)) {
						(Some(date), Some(dur)) => {
							let mut year = date.year();
							let mut month = date.month() as i32;
							let mut day = date.day();

							// Add months component
							let total_months = month + dur.get_months();
							year += (total_months - 1).div_euclid(12);
							month = (total_months - 1).rem_euclid(12) + 1;

							// Clamp day to valid range for the new month
							let max_day = days_in_month(year, month as u32);
							if day > max_day {
								day = max_day;
							}

							// Convert to days_since_epoch and add days component
							if let Some(base) = Date::new(year, month as u32, day) {
								let total_days = base.to_days_since_epoch()
									+ dur.get_days() + (dur.get_nanos()
									/ 86_400_000_000_000)
									as i32;
								match Date::from_days_since_epoch(total_days) {
									Some(result) => container.push(result),
									None => container.push_default(),
								}
							} else {
								container.push_default();
							}
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::Date(container)
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Duration],
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

		let final_data = match (date_bitvec, dur_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

fn days_in_month(year: i32, month: u32) -> u32 {
	match month {
		1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
		4 | 6 | 9 | 11 => 30,
		2 => {
			if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
				29
			} else {
				28
			}
		}
		_ => 0,
	}
}
