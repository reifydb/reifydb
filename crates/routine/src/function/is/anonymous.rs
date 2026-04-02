// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct IsAnonymous {
	info: FunctionInfo,
}

impl Default for IsAnonymous {
	fn default() -> Self {
		Self::new()
	}
}

impl IsAnonymous {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("is::anonymous"),
		}
	}
}

impl Function for IsAnonymous {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let is_anonymous = ctx.identity.is_anonymous();
		let row_count = ctx.row_count.max(1);
		let data: Vec<bool> = vec![is_anonymous; row_count];

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), ColumnData::bool(data))]))
	}
}
