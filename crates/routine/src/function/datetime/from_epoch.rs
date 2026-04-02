// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateTimeFromEpoch {
	info: FunctionInfo,
}

impl Default for DateTimeFromEpoch {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeFromEpoch {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("datetime::from_epoch"),
		}
	}
}

fn extract_i64(data: &ColumnData, i: usize) -> Option<i64> {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int8(c) => c.get(i).copied(),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as i64),
		_ => None,
	}
}

fn is_integer_type(data: &ColumnData) -> bool {
	matches!(
		data,
		ColumnData::Int1(_)
			| ColumnData::Int2(_) | ColumnData::Int4(_)
			| ColumnData::Int8(_) | ColumnData::Int16(_)
			| ColumnData::Uint1(_)
			| ColumnData::Uint2(_)
			| ColumnData::Uint4(_)
			| ColumnData::Uint8(_)
			| ColumnData::Uint16(_)
	)
}

impl Function for DateTimeFromEpoch {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
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

		if !is_integer_type(data) {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
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
				actual: data.get_type(),
			});
		}

		let mut container = TemporalContainer::with_capacity(row_count);

		for i in 0..row_count {
			if let Some(ts) = extract_i64(data, i) {
				if ts < 0 {
					return Err(FunctionError::ExecutionFailed {
						function: ctx.fragment.clone(),
						reason: format!(
							"datetime::from_epoch does not support negative timestamps: {}",
							ts
						),
					});
				}
				match DateTime::from_timestamp(ts) {
					Ok(dt) => container.push(dt),
					Err(_) => container.push_default(),
				}
			} else {
				container.push_default();
			}
		}

		let result_data = ColumnData::DateTime(container);

		let final_data = if let Some(bv) = bitvec {
			ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
