// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, datetime::DateTime, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateTimeSubtract {
	info: FunctionInfo,
}

impl Default for DateTimeSubtract {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeSubtract {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("datetime::subtract"),
		}
	}
}

impl Function for DateTimeSubtract {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let dt_col = &args[0];
		let dur_col = &args[1];
		let (dt_data, dt_bitvec) = dt_col.data().unwrap_option();
		let (dur_data, dur_bitvec) = dur_col.data().unwrap_option();
		let row_count = dt_data.len();

		let result_data = match (dt_data, dur_data) {
			(ColumnData::DateTime(dt_container), ColumnData::Duration(dur_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dt_container.get(i), dur_container.get(i)) {
						(Some(dt), Some(dur)) => {
							let date = dt.date();
							let time = dt.time();
							let mut year = date.year();
							let mut month = date.month() as i32;
							let mut day = date.day();

							// Subtract months component
							let total_months = month - dur.get_months();
							year += (total_months - 1).div_euclid(12);
							month = (total_months - 1).rem_euclid(12) + 1;

							// Clamp day to valid range for the new month
							let max_day = days_in_month(year, month as u32);
							if day > max_day {
								day = max_day;
							}

							// Convert to seconds since epoch and subtract day/nanos
							// components
							if let Some(base_date) = Date::new(year, month as u32, day) {
								let base_days = base_date.to_days_since_epoch() as i64
									- dur.get_days() as i64;
								let time_nanos = time.to_nanos_since_midnight() as i64
									- dur.get_nanos();

								let total_nanos = base_days as i128
									* 86_400_000_000_000i128 + time_nanos
									as i128;

								if total_nanos >= 0 && total_nanos <= u64::MAX as i128 {
									container.push(DateTime::from_nanos(
										total_nanos as u64,
									));
								} else {
									return Err(FunctionError::ExecutionFailed {
										function: ctx.fragment.clone(),
										reason: "datetime cannot be before Unix epoch".to_string(),
									});
								}
							} else {
								return Err(FunctionError::ExecutionFailed {
									function: ctx.fragment.clone(),
									reason: "datetime cannot be before Unix epoch"
										.to_string(),
								});
							}
						}
						_ => container.push_default(),
					}
				}

				ColumnData::DateTime(container)
			}
			(ColumnData::DateTime(_), other) => {
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
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (dt_bitvec, dur_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
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
