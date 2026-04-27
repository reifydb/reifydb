// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{Value, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct JsonObject {
	info: RoutineInfo,
}

impl Default for JsonObject {
	fn default() -> Self {
		Self::new()
	}
}

impl JsonObject {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("json::object"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for JsonObject {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
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

		if !unwrapped.len().is_multiple_of(2) {
			return Err(RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: "json::object requires an even number of arguments (key-value pairs)"
					.to_string(),
			});
		}

		// Validate that key columns (even indices) are Utf8
		for i in (0..unwrapped.len()).step_by(2) {
			let col_data = unwrapped[i];
			match col_data {
				ColumnBuffer::Utf8 {
					..
				} => {}
				other => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: i,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let row_count = if unwrapped.is_empty() {
			1
		} else {
			unwrapped[0].len()
		};
		let num_pairs = unwrapped.len() / 2;
		let mut results: Vec<Box<Value>> = Vec::with_capacity(row_count);

		for row in 0..row_count {
			let mut fields = Vec::with_capacity(num_pairs);
			for pair in 0..num_pairs {
				let key_data = unwrapped[pair * 2];
				let val_data = unwrapped[pair * 2 + 1];

				let key: String = key_data.get_as::<String>(row).unwrap_or_default();
				let value = val_data.get_value(row);

				fields.push((key, value));
			}
			results.push(Box::new(Value::Record(fields)));
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

impl Function for JsonObject {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
