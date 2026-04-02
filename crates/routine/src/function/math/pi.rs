// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::f64::consts::PI;

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Pi {
	info: FunctionInfo,
}

impl Default for Pi {
	fn default() -> Self {
		Self::new()
	}
}

impl Pi {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::pi"),
		}
	}
}

impl Function for Pi {
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

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), ColumnData::float8(vec![PI]))]))
	}
}
