// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DurationScale {
	info: FunctionInfo,
}

impl Default for DurationScale {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationScale {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("duration::scale"),
		}
	}
}

fn extract_i64(data: &ColumnBuffer, i: usize) -> Option<i64> {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Int8(c) => c.get(i).copied(),
		ColumnBuffer::Int16(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as i64),
		ColumnBuffer::Uint16(c) => c.get(i).map(|&v| v as i64),
		_ => None,
	}
}

fn is_integer_type(data: &ColumnBuffer) -> bool {
	matches!(
		data,
		ColumnBuffer::Int1(_)
			| ColumnBuffer::Int2(_)
			| ColumnBuffer::Int4(_)
			| ColumnBuffer::Int8(_)
			| ColumnBuffer::Int16(_)
			| ColumnBuffer::Uint1(_)
			| ColumnBuffer::Uint2(_)
			| ColumnBuffer::Uint4(_)
			| ColumnBuffer::Uint8(_)
			| ColumnBuffer::Uint16(_)
	)
}

impl Function for DurationScale {
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

		let dur_col = &args[0];
		let scalar_col = &args[1];

		let (dur_data, dur_bv) = dur_col.data().unwrap_option();
		let (scalar_data, scalar_bv) = scalar_col.data().unwrap_option();

		match dur_data {
			ColumnBuffer::Duration(dur_container) => {
				if !is_integer_type(scalar_data) {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![
							Type::Int1,
							Type::Int2,
							Type::Int4,
							Type::Int8,
							Type::Int16,
							Type::Uint1,
							Type::Uint2,
							Type::Uint4,
							Type::Uint8,
							Type::Uint16,
						],
						actual: scalar_data.get_type(),
					});
				}

				let row_count = dur_data.len();
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (dur_container.get(i), extract_i64(scalar_data, i)) {
						(Some(dur), Some(scalar)) => {
							container.push(*dur * scalar);
						}
						_ => container.push_default(),
					}
				}

				let mut result_data = ColumnBuffer::Duration(container);
				if let Some(bv) = dur_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				} else if let Some(bv) = scalar_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
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
