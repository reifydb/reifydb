// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DurationNegate {
	info: FunctionInfo,
}

impl Default for DurationNegate {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationNegate {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("duration::negate"),
		}
	}
}

impl Function for DurationNegate {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
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
		let (data, bitvec) = column.data().unwrap_option();
		let row_count = data.len();

		match data {
			ColumnData::Duration(container_in) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(val) = container_in.get(i) {
						container.push(val.negate());
					} else {
						container.push_default();
					}
				}

				let mut result_data = ColumnData::Duration(container);
				if let Some(bv) = bitvec {
					result_data = ColumnData::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), result_data)]))
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
		}
	}
}
