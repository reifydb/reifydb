// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::time_array, datetime::DateTime, value_type::ValueType};

use crate::function::support::column::array_column;

pub struct TimeNow {
	info: RoutineInfo,
}

impl Default for TimeNow {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeNow {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeNow {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let millis = ctx.runtime_context.clock.now().to_millis();
		let dt = DateTime::from_epoch_millis(millis)?;
		let time = dt.time();

		let container = vec![time];

		Ok(array_column(ctx.fragment.text(), ValueType::Time, Arc::new(time_array(container))))
	}
}

impl Function for TimeNow {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
