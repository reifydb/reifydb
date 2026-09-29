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
	container::temporal_array::{duration_array, times},
	duration::Duration,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

pub struct TimeAge {
	info: RoutineInfo,
}

impl Default for TimeAge {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeAge {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::age"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeAge {
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

		match (&data1.data, &data2.data) {
			(ViewData::Time(container1), ViewData::Time(container2)) => {
				let row_count = data1.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (times(container1).get(i), times(container2).get(i)) {
						(Some(t1), Some(t2)) => {
							let diff_nanos = t1.to_nanos_since_midnight() as i64
								- t2.to_nanos_since_midnight() as i64;
							container.push(Duration::from_nanoseconds(diff_nanos)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				Ok(array_column(
					ctx.fragment.text(),
					ValueType::Duration,
					Arc::new(duration_array(container)),
				))
			}
			(ViewData::Time(_), _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Time],
				actual: data2.get_type(),
			}),
			(_, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Time],
				actual: data1.get_type(),
			}),
		}
	}
}

impl Function for TimeAge {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
