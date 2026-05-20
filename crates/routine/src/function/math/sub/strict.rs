// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::r#type::Type;

use crate::{
	function::math::arith::{dispatch::dispatch_strict, op::Sub},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct SubStrict {
	info: RoutineInfo,
}

impl Default for SubStrict {
	fn default() -> Self {
		Self::new()
	}
}

impl SubStrict {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sub_strict"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for SubStrict {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		dispatch_strict::<Sub>(ctx, args)
	}
}

impl Function for SubStrict {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
