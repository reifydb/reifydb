// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct IsSome {
	info: FunctionInfo,
}

impl Default for IsSome {
	fn default() -> Self {
		Self::new()
	}
}

impl IsSome {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("is::some"),
		}
	}
}

impl Function for IsSome {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let row_count = column.data().len();
		let data: Vec<bool> = (0..row_count).map(|i| column.data().is_defined(i)).collect();

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), ColumnData::bool(data))]))
	}
}
