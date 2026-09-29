// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::duration;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::durations,
	duration::Duration,
	value_type::ValueType,
};

pub struct DurationAdd {
	info: RoutineInfo,
}

impl Default for DurationAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationAdd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::add"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationAdd {
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
		let lhs_data = ColumnView::try_from(&args[0])?;
		let rhs_data = ColumnView::try_from(&args[1])?;

		match (&lhs_data.data, &rhs_data.data) {
			(ViewData::Duration(lhs_container), ViewData::Duration(rhs_container)) => {
				let row_count = lhs_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (durations(lhs_container).get(i), durations(rhs_container).get(i)) {
						(Some(lv), Some(rv)) => {
							container.push(lv.try_add(*rv)?);
						}
						_ => container.push(Duration::default()),
					}
				}

				let result_data = duration(ctx.fragment.text(), container);
				Ok(result_data)
			}
			(ViewData::Duration(_), _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Duration],
				actual: rhs_data.get_type(),
			}),
			(_, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: lhs_data.get_type(),
			}),
		}
	}
}

impl Function for DurationAdd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
