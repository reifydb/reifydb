// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{Value, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct JsonArray {
	info: RoutineInfo,
}

impl Default for JsonArray {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonArray {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("json::array"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for JsonArray {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::any(vec![Box::new(Value::List(vec![]))]),
			)]));
		}

		let mut unwrapped: Vec<_> = Vec::with_capacity(args.len());
		let mut combined_bv: Option<BitVec> = None;

		for col in args.iter() {
			let (data, bitvec) = col.data().unwrap_option();
			if let Some(bv) = bitvec {
				combined_bv = Some(match combined_bv {
					Some(existing) => existing.and(bv),
					None => bv.clone(),
				});
			}
			unwrapped.push(data);
		}

		let row_count = unwrapped[0].len();
		let mut results: Vec<Box<Value>> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut items = Vec::with_capacity(unwrapped.len());
			for col_data in unwrapped.iter() {
				items.push(col_data.get_value(row));
			}
			results.push(Box::new(Value::List(items)));
		}

		let result_data = ColumnBuffer::any(results);
		let final_data = match combined_bv {
			Some(bv) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv,
			},
			None => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for JsonArray {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
