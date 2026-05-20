// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::r#type::Type;

use crate::{
	function::math::arith::{cast::promote_two, dispatch::dispatch_default, op::Add},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct AddDefault {
	info: RoutineInfo,
}

impl Default for AddDefault {
	fn default() -> Self {
		Self::new()
	}
}

impl AddDefault {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::add_default"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for AddDefault {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		if input_types.len() >= 2 {
			promote_two(input_types[0].clone(), input_types[1].clone())
		} else {
			input_types.first().cloned().unwrap_or(Type::Float8)
		}
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		dispatch_default::<Add>(ctx, args)
	}
}

impl Function for AddDefault {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
