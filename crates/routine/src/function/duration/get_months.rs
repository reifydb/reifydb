// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::durations,
	value_type::ValueType,
};

pub struct DurationGetMonths {
	info: RoutineInfo,
}

impl Default for DurationGetMonths {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationGetMonths {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::get_months"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationGetMonths {
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

		match &data.data {
			ViewData::Duration(container) => {
				let mut result = Vec::with_capacity(row_count);

				for dur in durations(container) {
					result.push(dur.get_months());
				}

				let result_data = int4(ctx.fragment.text(), result);
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

impl Function for DurationGetMonths {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
