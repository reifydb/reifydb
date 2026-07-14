// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::value::value_type::ValueType;

use crate::{
	function::vector::{kernel, support::prepare_pair},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct VectorCosineDistance {
	info: RoutineInfo,
}

impl Default for VectorCosineDistance {
	fn default() -> Self {
		Self::new()
	}
}

impl VectorCosineDistance {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("vector::cosine_distance"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for VectorCosineDistance {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Float8
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let (left, right, defined) = prepare_pair(ctx, args)?;

		let rows = defined.len();
		let mut values = Vec::with_capacity(rows);
		let mut bitvec = Vec::with_capacity(rows);

		for (i, is_defined) in defined.iter().enumerate() {
			match (*is_defined, left.get(i), right.get(i)) {
				(true, Some(l), Some(r)) => match kernel::cosine_distance(l, r) {
					Some(distance) => {
						values.push(distance as f64);
						bitvec.push(true);
					}
					None => {
						values.push(0.0);
						bitvec.push(false);
					}
				},
				_ => {
					values.push(0.0);
					bitvec.push(false);
				}
			}
		}

		let data = ColumnBuffer::float8_with_bitvec(values, bitvec);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), data)]))
	}
}

impl Function for VectorCosineDistance {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
