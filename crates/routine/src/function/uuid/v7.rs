// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::varlen_array, uuid::Uuid7, value_type::ValueType};
use uuid::Uuid;

pub struct UuidV7 {
	info: RoutineInfo,
}

impl Default for UuidV7 {
	fn default() -> Self {
		Self::new()
	}
}

impl UuidV7 {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("uuid::v7"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for UuidV7 {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Uuid7
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			let mut generated = Vec::with_capacity(ctx.row_count);
			for _ in 0..ctx.row_count {
				generated.push(Uuid7::generate(&ctx.runtime_context.clock, &ctx.runtime_context.rng));
			}
			let result_data = ColumnBuffer::uuid7(generated);
			return Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]));
		}

		let data = &args[0];
		let row_count = data.len();
		let nulls = data.nulls();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if nulls.is_some_and(|nulls| nulls.is_null(i)) {
						result.push(Uuid7::default());
						res_bitvec.push(false);
						continue;
					}
					let s = varlen_array::get(container, i).unwrap();
					let parsed = Uuid::parse_str(s).map_err(|e| {
						RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("invalid UUID string '{}': {}", s, e),
						}
					})?;
					if parsed.get_version_num() != 7 {
						return Err(RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"expected UUID v7, got v{}",
								parsed.get_version_num()
							),
						});
					}
					result.push(Uuid7::from(parsed));
					res_bitvec.push(true);
				}
				let result_data = match nulls {
					Some(_) => ColumnBuffer::uuid7_with_bitvec(result, res_bitvec),
					None => ColumnBuffer::uuid7(result),
				};
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
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

impl Function for UuidV7 {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Range(0, 1)
	}
}
