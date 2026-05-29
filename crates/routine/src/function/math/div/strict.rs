// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::value_type::ValueType;

use crate::{
	function::math::arith::{dispatch::dispatch_strict, op::Div},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct DivStrict {
	info: RoutineInfo,
}

impl Default for DivStrict {
	fn default() -> Self {
		Self::new()
	}
}

impl DivStrict {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::div_strict"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DivStrict {
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
		dispatch_strict::<Div>(ctx, args)
	}
}

impl Function for DivStrict {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
