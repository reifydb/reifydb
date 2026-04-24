// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateStartOfYear {
	info: FunctionInfo,
}

impl Default for DateStartOfYear {
	fn default() -> Self {
		Self::new()
	}
}

impl DateStartOfYear {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("date::start_of_year"),
		}
	}
}

impl Function for DateStartOfYear {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
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
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		let result_data = match data {
			ColumnBuffer::Date(container) => {
				let mut result = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(date) = container.get(i) {
						match Date::new(date.year(), 1, 1) {
							Some(d) => result.push(d),
							None => result.push_default(),
						}
					} else {
						result.push_default();
					}
				}

				ColumnBuffer::Date(result)
			}
			other => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = if let Some(bv) = bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
