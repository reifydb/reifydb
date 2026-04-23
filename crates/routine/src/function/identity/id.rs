// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Id {
	info: FunctionInfo,
}

impl Default for Id {
	fn default() -> Self {
		Self::new()
	}
}

impl Id {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("identity::id"),
		}
	}
}

impl Function for Id {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::IdentityId
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
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
				ColumnBuffer::none_typed(Type::IdentityId, row_count),
			)]));
		}

		Ok(Columns::new(vec![ColumnWithName::new(
			ctx.fragment.clone(),
			ColumnBuffer::identity_id(vec![identity; row_count]),
		)]))
	}
}
