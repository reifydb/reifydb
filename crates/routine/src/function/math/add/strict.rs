// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::value_type::ValueType;

use crate::{
	function::math::arith::{dispatch::dispatch_strict, op::Add},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct AddStrict {
	info: RoutineInfo,
}

impl Default for AddStrict {
	fn default() -> Self {
		Self::new()
	}
}

impl AddStrict {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::add_strict"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for AddStrict {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().cloned().unwrap_or(ValueType::Float8)
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		dispatch_strict::<Add>(ctx, args)
	}
}

impl Function for AddStrict {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
