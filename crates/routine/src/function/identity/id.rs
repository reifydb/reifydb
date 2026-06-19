// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::value_type::ValueType;

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::IdentityId
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if !args.is_empty() {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let identity = ctx.identity;
		let row_count = ctx.row_count.max(1);
		if identity.is_anonymous() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::none_typed(ValueType::IdentityId, row_count),
			)]));
		}

		Ok(Columns::new(vec![ColumnWithName::new(
			ctx.fragment.clone(),
			ColumnBuffer::identity_id(vec![identity; row_count]),
		)]))
	}
}

impl Function for Id {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
