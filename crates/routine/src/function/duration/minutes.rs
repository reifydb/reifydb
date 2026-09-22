// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::{decimal_array::u128_at, temporal_array::duration_array},
	duration::Duration,
	value_type::ValueType,
};

pub struct DurationMinutes {
	info: RoutineInfo,
}

impl Default for DurationMinutes {
	fn default() -> Self {
		Self::new()
	}
}

impl DurationMinutes {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("duration::minutes"),
		}
	}
}

fn extract_i64(data: &ColumnBuffer, i: usize) -> Option<i64> {
	match data {
		ColumnBuffer::Int1(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Int2(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Int4(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Int8(c) => c.values().get(i).copied(),
		ColumnBuffer::Int16(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Uint1(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Uint2(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Uint4(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Uint8(c) => c.values().get(i).map(|&v| v as i64),
		ColumnBuffer::Uint16(c) => u128_at(c, i).map(|v| v as i64),
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

impl<'a> Routine<FunctionContext<'a>> for DurationMinutes {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Duration
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		if !is_integer_type(data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					ValueType::Int1,
					ValueType::Int2,
					ValueType::Int4,
					ValueType::Int8,
					ValueType::Int16,
					ValueType::Uint1,
					ValueType::Uint2,
					ValueType::Uint4,
					ValueType::Uint8,
					ValueType::Uint16,
				],
				actual: data.get_type(),
			});
		}

		let mut container = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if let Some(val) = extract_i64(data, i) {
				container.push(Duration::from_minutes(val)?);
			} else {
				container.push(Duration::default());
			}
		}

		let mut result_data = ColumnBuffer::Duration(duration_array(container));
		if let Some(bv) = bitvec {
			result_data = ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			};
		}
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for DurationMinutes {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
