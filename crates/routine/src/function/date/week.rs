// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	fragment::Fragment,
	value::{date::Date, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct DateWeek {
	info: RoutineInfo,
}

impl Default for DateWeek {
	fn default() -> Self {
		Self::new()
	}
}

impl DateWeek {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::week"),
		}
	}
}

/// Compute the ISO 8601 week number for a date.
///
/// ISO 8601 rules:
/// - Weeks start on Monday
/// - Week 1 is the week containing January 4th
/// - A year has 52 or 53 weeks
/// - Jan 1-3 may belong to week 52/53 of the previous year
/// - Dec 29-31 may belong to week 1 of the next year
fn iso_week_number(date: &Date) -> Result<i32, RoutineError> {
	let days = date.to_days_since_epoch();

	// ISO day of week: Mon=1..Sun=7
	let dow = ((days % 7 + 3) % 7 + 7) % 7 + 1;

	// Find the Thursday of this date's week (ISO weeks are identified by their Thursday)
	let thursday = days + (4 - dow);

	// Find Jan 1 of the year containing that Thursday
	let thursday_ymd = {
		let d = Date::from_days_since_epoch(thursday).ok_or_else(|| RoutineError::FunctionExecutionFailed {
			function: Fragment::internal("date::week"),
			reason: "failed to compute date from days since epoch".to_string(),
		})?;
		d.year()
	};
	let jan1 = Date::new(thursday_ymd, 1, 1).ok_or_else(|| RoutineError::FunctionExecutionFailed {
		function: Fragment::internal("date::week"),
		reason: "failed to construct Jan 1 date".to_string(),
	})?;
	let jan1_days = jan1.to_days_since_epoch();

	// Week number = how many weeks between Jan 1 of that year and the Thursday

	Ok((thursday - jan1_days) / 7 + 1)
}

impl<'a> Routine<FunctionContext<'a>> for DateWeek {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Date(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = container.get(i) {
						result.push(iso_week_number(date)?);
						res_bitvec.push(true);
					} else {
						result.push(0);
						res_bitvec.push(false);
					}
				}

				ColumnBuffer::int4_with_bitvec(result, res_bitvec)
			}
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = if let Some(bv) = bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateWeek {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
