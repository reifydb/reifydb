// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::f64::consts::E;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Euler {
	info: FunctionInfo,
}

impl Default for Euler {
	fn default() -> Self {
		Self::new()
	}
}

impl Euler {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::e"),
		}
	}
}

impl Function for Euler {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Float8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::float8(vec![E]))]))
	}
}
