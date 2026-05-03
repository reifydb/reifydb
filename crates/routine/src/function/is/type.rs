// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{Value, r#type::Type};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct IsType {
	info: RoutineInfo,
}

impl Default for IsType {
	fn default() -> Self {
		Self::new()
	}
}

impl IsType {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::type"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsType {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let value_column = &args[0];
		let type_column = &args[1];
		let row_count = value_column.len();

		let target_type = match type_column.get_value(0) {
			Value::Any(boxed) => match boxed.as_ref() {
				Value::Type(t) => t.clone(),
				_ => {
					return Err(RoutineError::FunctionInvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: 1,
						expected: vec![Type::Any],
						actual: boxed.get_type(),
					});
				}
			},
			Value::None {
				..
			} => Type::Option(Box::new(Type::Any)),
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Any],
					actual: other.get_type(),
				});
			}
		};

		let data: Vec<bool> = (0..row_count)
			.map(|i| {
				let vtype = value_column.get_value(i).get_type();
				if target_type == Type::Option(Box::new(Type::Any)) {
					vtype.is_option()
				} else {
					!vtype.is_option() && vtype.inner_type() == target_type.inner_type()
				}
			})
			.collect();

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::bool(data))]))
	}
}

impl Function for IsType {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
