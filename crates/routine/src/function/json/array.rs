// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{Value, r#type::Type},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct JsonArray {
	info: FunctionInfo,
}

impl Default for JsonArray {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonArray {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("json::array"),
		}
	}
}

impl Function for JsonArray {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.is_empty() {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::any(vec![Box::new(Value::List(vec![]))]),
			)]));
		}

		// Check for any option columns and unwrap them
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
