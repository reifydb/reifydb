// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::{identity_id, none_typed};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct Id {
	info: RoutineInfo,
}

impl Default for Id {
	fn default() -> Self {
		Self::new()
	}
}

impl Id {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("identity::id"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Id {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::IdentityId
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let identity = ctx.identity;
		let row_count = ctx.row_count.max(1);
		if identity.is_anonymous() {
			return Ok(none_typed(ctx.fragment.text(), ValueType::IdentityId, row_count));
		}

		Ok(identity_id(ctx.fragment.text(), vec![identity; row_count]))
	}
}

impl Function for Id {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
