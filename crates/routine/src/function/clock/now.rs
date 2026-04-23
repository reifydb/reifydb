// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Now {
	info: FunctionInfo,
}

impl Default for Now {
	fn default() -> Self {
		Self::new()
	}
}

impl Now {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("clock::now"),
		}
	}
}

impl Function for Now {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let millis = ctx.runtime_context.clock.now_millis() as i64;
		let row_count = ctx.row_count.max(1);
		let data = vec![millis; row_count];
		let bitvec = vec![true; row_count];

		let result_data = ColumnBuffer::int8_with_bitvec(data, bitvec);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}
