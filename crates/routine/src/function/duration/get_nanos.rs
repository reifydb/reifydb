// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int8;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::durations,
	value_type::ValueType,
};

pub struct DurationGetNanos {
	info: RoutineInfo,
}

impl Default for DurationGetNanos {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationGetNanos {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::get_nanos"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DurationGetNanos {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
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
					result.push(dur.get_nanos());
				}

				let result_data = int8(ctx.fragment.text(), result);
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

impl Function for DurationGetNanos {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
