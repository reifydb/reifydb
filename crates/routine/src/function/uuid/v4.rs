// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{r#type::Type, uuid::Uuid4};
use uuid::{Builder, Uuid};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct UuidV4 {
	info: RoutineInfo,
}

impl Default for UuidV4 {
	fn default() -> Self {
		Self::new()
	}
}

impl UuidV4 {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("uuid::v4"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for UuidV4 {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Uuid4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() > 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		if args.is_empty() {
			let bytes = ctx.env.runtime_context.rng.bytes_16();
			let uuid = Uuid4::from(Builder::from_random_bytes(bytes).into_uuid());
			let result_data = ColumnBuffer::uuid4(vec![uuid]);
			return Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]));
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
						Uuid::parse_str(s).map_err(|e| RoutineError::FunctionExecutionFailed {
							function: ctx.env.fragment.clone(),
							reason: format!("invalid UUID string '{}': {}", s, e),
						})?;
					if parsed.get_version_num() != 4 {
						return Err(RoutineError::FunctionExecutionFailed {
							function: ctx.env.fragment.clone(),
							reason: format!(
								"expected UUID v4, got v{}",
								parsed.get_version_num()
							),
						});
					}
					result.push(Uuid4::from(parsed));
				}
				let result_data = ColumnBuffer::uuid4(result);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					},
					None => result_data,
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
