// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{r#type::Type, uuid::Uuid7};
use uuid::Uuid;

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct UuidV7 {
	info: FunctionInfo,
}

impl Default for UuidV7 {
	fn default() -> Self {
		Self::new()
	}
}

impl UuidV7 {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("uuid::v7"),
		}
	}
}

impl Function for UuidV7 {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Uuid7
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() > 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		if args.is_empty() {
			let uuid = Uuid7::generate(&ctx.runtime_context.clock, &ctx.runtime_context.rng);
			let result_data = ColumnBuffer::uuid7(vec![uuid]);
			return Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]));
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(row_count);
				for i in 0..row_count {
					let s = &container[i];
					let parsed =
						Uuid::parse_str(s).map_err(|e| FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("invalid UUID string '{}': {}", s, e),
						})?;
					if parsed.get_version_num() != 7 {
						return Err(FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"expected UUID v7, got v{}",
								parsed.get_version_num()
							),
						});
					}
					result.push(Uuid7::from(parsed));
				}
				let result_data = ColumnBuffer::uuid7(result);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					},
					None => result_data,
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
