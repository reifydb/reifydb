// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{container::varlen_array, uuid::Uuid4, value_type::ValueType};
use uuid::{Builder, Uuid};

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Uuid4
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.is_empty() {
			let mut generated = Vec::with_capacity(ctx.row_count);
			for _ in 0..ctx.row_count {
				let bytes = ctx.runtime_context.rng.bytes_16();
				generated.push(Uuid4::from(Builder::from_random_bytes(bytes).into_uuid()));
			}
			let result_data = ColumnBuffer::uuid4(generated);
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
						result.push(Uuid4::default());
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
					if parsed.get_version_num() != 4 {
						return Err(RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!(
								"expected UUID v4, got v{}",
								parsed.get_version_num()
							),
						});
					}
					result.push(Uuid4::from(parsed));
					res_bitvec.push(true);
				}
				let result_data = match nulls {
					Some(_) => ColumnBuffer::uuid4_with_bitvec(result, res_bitvec),
					None => ColumnBuffer::uuid4(result),
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

impl Function for UuidV4 {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Range(0, 1)
	}
}
