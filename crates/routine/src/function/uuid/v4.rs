// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{uuid4, uuid4_with_bitvec};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::varlen_array,
	uuid::Uuid4,
	value_type::ValueType,
};
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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		if args.is_empty() {
			let mut generated = Vec::with_capacity(ctx.row_count);
			for _ in 0..ctx.row_count {
				let bytes = ctx.runtime_context.rng.bytes_16();
				generated.push(Uuid4::from(Builder::from_random_bytes(bytes).into_uuid()));
			}
			return Ok(uuid4(ctx.fragment.text(), generated));
		}

		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();
		let nulls = data.logical_nulls();

		match &data.data {
			ViewData::Utf8 {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if nulls.as_ref().is_some_and(|nulls| nulls.is_null(i)) {
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
				Ok(match nulls {
					Some(_) => uuid4_with_bitvec(ctx.fragment.text(), result, res_bitvec),
					None => uuid4(ctx.fragment.text(), result),
				})
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: data.get_type(),
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

	fn has_fixed_return_type(&self) -> bool {
		true
	}
}
