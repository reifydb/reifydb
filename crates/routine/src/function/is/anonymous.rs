// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::bool;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct IsAnonymous {
	info: RoutineInfo,
}

impl Default for IsAnonymous {
	fn default() -> Self {
		Self::new()
	}
}

impl IsAnonymous {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("is::anonymous"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for IsAnonymous {
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
		let is_anonymous = ctx.identity.is_anonymous();
		let row_count = ctx.row_count.max(1);
		let data: Vec<bool> = vec![is_anonymous; row_count];

		Ok(bool(ctx.fragment.text(), data))
	}
}

impl Function for IsAnonymous {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
