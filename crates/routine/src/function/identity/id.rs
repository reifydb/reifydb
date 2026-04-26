// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Id {
	info: RoutineInfo,
}

impl Default for Id {
	fn default() -> Self {
		Self::new()
	}
}

impl Id {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("identity::id"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Id {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::IdentityId
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let identity = ctx.env.identity;
		let row_count = ctx.env.row_count.max(1);
		if identity.is_anonymous() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.env.fragment.clone(),
				ColumnBuffer::none_typed(Type::IdentityId, row_count),
			)]));
		}

		Ok(Columns::new(vec![ColumnWithName::new(
			ctx.env.fragment.clone(),
			ColumnBuffer::identity_id(vec![identity; row_count]),
		)]))
	}
}
