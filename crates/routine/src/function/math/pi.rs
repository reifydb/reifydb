// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::f64::consts::PI;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Pi {
	info: RoutineInfo,
}

impl Default for Pi {
	fn default() -> Self {
		Self::new()
	}
}

impl Pi {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::pi"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Pi {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Float8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), ColumnBuffer::float8(vec![PI]))]))
	}
}
