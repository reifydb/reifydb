// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::f64::consts::E;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct Euler {
	info: RoutineInfo,
}

impl Default for Euler {
	fn default() -> Self {
		Self::new()
	}
}

impl Euler {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::e"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Euler {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Float8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::float8(vec![E]))]))
	}
}

impl Function for Euler {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
