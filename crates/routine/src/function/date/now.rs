// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::temporal_array::date_array, datetime::DateTime, value_type::ValueType};

use crate::function::support::column::array_column;

pub struct DateNow {
	info: RoutineInfo,
}

impl Default for DateNow {
	fn default() -> Self {
		Self::new()
	}
}

impl DateNow {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::now"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateNow {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Date
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let row_count = ctx.row_count.max(1);

		let millis = ctx.runtime_context.clock.now().to_millis();
		let dt = DateTime::from_epoch_millis(millis)?;
		let date = dt.date();

		let mut container = Vec::with_capacity(row_count);
		for _ in 0..row_count {
			container.push(date);
		}

		Ok(array_column(ctx.fragment.text(), ValueType::Date, Arc::new(date_array(container))))
	}
}

impl Function for DateNow {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
