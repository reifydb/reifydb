// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::bool;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct IsRoot {
	info: RoutineInfo,
}

impl Default for IsRoot {
	fn default() -> Self {
		Self::new()
	}
}

impl IsRoot {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::root"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsRoot {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Boolean
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let is_root = ctx.identity.is_root();
		let row_count = ctx.row_count.max(1);
		let data: Vec<bool> = vec![is_root; row_count];

		Ok(bool(ctx.fragment.text(), data))
	}
}

impl Function for IsRoot {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
