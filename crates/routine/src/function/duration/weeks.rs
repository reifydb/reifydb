// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, duration::Duration, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct DurationWeeks {
	info: RoutineInfo,
}

impl Default for DurationWeeks {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationWeeks {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::weeks"),
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

impl<'a> Routine<FunctionContext<'a>> for DurationWeeks {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		if !is_integer_type(data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
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
			if let Some(val) = extract_i64(data, i) {
				container.push(Duration::from_weeks(val)?);
			} else {
				container.push_default();
			}
		}

		let mut result_data = ColumnBuffer::Duration(container);
		if let Some(bv) = bitvec {
			result_data = ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			};
		}
		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
	}
}
