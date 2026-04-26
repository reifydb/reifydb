// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	fragment::Fragment,
	value::{blob::Blob, r#type::Type},
};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

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

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Blob
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

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let b64_str = &container[i];
						let blob = Blob::from_b64(Fragment::internal(b64_str))?;
						result_data.push(blob);
						result_bitvec.push(true);
					} else {
						result_data.push(Blob::empty());
						result_bitvec.push(false);
					}
				}

				let result_col_data = ColumnBuffer::blob_with_bitvec(result_data, result_bitvec);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_col_data),
						bitvec: bv.clone(),
					},
					None => result_col_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
