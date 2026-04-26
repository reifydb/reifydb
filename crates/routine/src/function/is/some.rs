// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::Type;

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct IsSome {
	info: RoutineInfo,
}

impl Default for IsSome {
	fn default() -> Self {
		Self::new()
	}
}

impl IsSome {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::some"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsSome {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Boolean
	}

	fn propagates_options(&self) -> bool {
		false
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
		let row_count = column.len();
		let data: Vec<bool> = (0..row_count).map(|i| column.is_defined(i)).collect();

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), ColumnBuffer::bool(data))]))
	}
}
