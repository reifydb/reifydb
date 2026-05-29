// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::value_type::ValueType;

use crate::{
	function::math::arith::{dispatch::dispatch_strict, op::Rem},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct RemStrict {
	info: RoutineInfo,
}

impl Default for RemStrict {
	fn default() -> Self {
		Self::new()
	}
}

impl RemStrict {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::rem_strict"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for RemStrict {
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
		dispatch_strict::<Rem>(ctx, args)
	}
}

impl Function for RemStrict {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
