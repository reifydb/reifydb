// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct IsAnonymous {
	info: RoutineInfo,
}

impl Default for IsAnonymous {
	fn default() -> Self {
		Self::new()
	}
}

impl IsAnonymous {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::anonymous"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsAnonymous {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let is_anonymous = ctx.identity.is_anonymous();
		let row_count = ctx.row_count.max(1);
		let data: Vec<bool> = vec![is_anonymous; row_count];

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::bool(data))]))
	}
}

impl Function for IsAnonymous {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
