// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4_with_bitvec;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		container::temporal_array::datetimes,
		date::Date,
		value_type::ValueType,
	},
};

pub struct DateTimeWeek {
	info: RoutineInfo,
}

impl Default for DateTimeWeek {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeWeek {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::week"),
		}
	}
}

fn iso_week_number(date: &Date) -> Result<i32, RoutineError> {
	let days = date.to_days_since_epoch();

	let dow = ((days % 7 + 3) % 7 + 7) % 7 + 1;

	let thursday = days + (4 - dow);

	let thursday_year = {
		let d = Date::from_days_since_epoch(thursday).ok_or_else(|| RoutineError::FunctionExecutionFailed {
			function: Fragment::internal("datetime::week"),
			reason: "failed to compute date from days since epoch".to_string(),
		})?;
		d.year()
	};
	let jan1 = Date::new(thursday_year, 1, 1).ok_or_else(|| RoutineError::FunctionExecutionFailed {
		function: Fragment::internal("datetime::week"),
		reason: "failed to construct Jan 1 date".to_string(),
	})?;
	let jan1_days = jan1.to_days_since_epoch();

	Ok((thursday - jan1_days) / 7 + 1)
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeWeek {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::DateTime(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = datetimes(container).get(i) {
						let date = dt.date();
						result.push(iso_week_number(&date)?);
						res_bitvec.push(true);
					} else {
						result.push(0);
						res_bitvec.push(false);
					}
				}

				int4_with_bitvec(ctx.fragment.text(), result, res_bitvec)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateTimeWeek {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
