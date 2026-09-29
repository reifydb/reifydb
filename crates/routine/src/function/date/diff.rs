// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::{dates, duration_array},
	duration::Duration,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct DateDiff {
	info: RoutineInfo,
}

impl Default for DateDiff {
	fn default() -> Self {
		Self::new()
	}
}

impl DateDiff {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::diff"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateDiff {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data1 = ColumnView::try_from(&args[0])?;
		let data2 = ColumnView::try_from(&args[1])?;
		let row_count = data1.len();

		let result_data = match (&data1.data, &data2.data) {
			(ViewData::Date(container1), ViewData::Date(container2)) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dates(container1).get(i), dates(container2).get(i)) {
						(Some(d1), Some(d2)) => {
							let diff_days = (d1.to_days_since_epoch()
								- d2.to_days_since_epoch())
								as i64;
							container.push(Duration::from_days(diff_days)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				array_column(
					ctx.fragment.text(),
					ValueType::Duration,
					Arc::new(duration_array(container)),
				)
			}
			(ViewData::Date(_), _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::Date],
					actual: data2.get_type(),
				});
			}
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::Date],
					actual: data1.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateDiff {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
