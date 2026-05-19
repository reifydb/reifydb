// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_type::value::r#type::Type;

use crate::{
	function::math::arith::{
		cast::promote_two,
		dispatch::{BasicStrategy, dispatch_two},
		op::Add,
	},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct AddSaturate {
	info: RoutineInfo,
}

impl Default for AddSaturate {
	fn default() -> Self {
		Self::new()
	}
}

impl AddSaturate {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::add_saturate"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for AddSaturate {
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
		dispatch_two::<Add>(ctx, args, BasicStrategy::Saturate)
	}
}

impl Function for AddSaturate {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
