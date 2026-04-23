// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateTimeDiff {
	info: FunctionInfo,
}

impl Default for DateTimeDiff {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeDiff {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("datetime::diff"),
		}
	}
}

impl Function for DateTimeDiff {
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
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let col1 = &args[0];
		let col2 = &args[1];
		let (data1, bitvec1) = col1.data().unwrap_option();
		let (data2, bitvec2) = col2.data().unwrap_option();
		let row_count = data1.len();

		let result_data = match (data1, data2) {
			(ColumnBuffer::DateTime(container1), ColumnBuffer::DateTime(container2)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (container1.get(i), container2.get(i)) {
						(Some(dt1), Some(dt2)) => {
							let diff_nanos = dt1.to_nanos() as i64 - dt2.to_nanos() as i64;
							container.push(Duration::from_nanoseconds(diff_nanos)?);
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::Duration(container)
			}
			(ColumnBuffer::DateTime(_), other) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (bitvec1, bitvec2) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
