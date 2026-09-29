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

pub struct DurationNegate {
	info: RoutineInfo,
}

impl Default for DurationNegate {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationNegate {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::negate"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationNegate {
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
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		match &data.data {
			ViewData::Duration(container_in) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(val) = durations(container_in).get(i) {
						container.push(val.negate()?);
					} else {
						container.push(Duration::default());
					}
				}

				let result_data = duration(ctx.fragment.text(), container);
				Ok(result_data)
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Duration],
				actual: data.get_type(),
			}),
		}
	}
}

impl Function for DurationNegate {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
