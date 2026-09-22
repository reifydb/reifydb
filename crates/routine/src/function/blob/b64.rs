// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::Array;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	fragment::Fragment,
	value::{blob::Blob, value_type::ValueType},
};

pub struct BlobB64 {
	info: RoutineInfo,
}

impl Default for BlobB64 {
	fn default() -> Self {
		Self::new()
	}
}

impl BlobB64 {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("blob::b64"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for BlobB64 {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Blob
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.len());

				for i in 0..row_count {
					let b64_str = container.value(i);
					let blob = Blob::from_b64(Fragment::internal(b64_str))?;
					result_data.push(blob);
				}

				let result_col_data = ColumnBuffer::blob(result_data);
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_col_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for BlobB64 {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
