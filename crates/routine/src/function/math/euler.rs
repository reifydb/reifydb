// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::E;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::float8;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct Euler {
	info: RoutineInfo,
}

impl Default for Euler {
	fn default() -> Self {
		Self::new()
	}
}

impl Euler {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::e"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Euler {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Float8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		_args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		Ok(float8(ctx.fragment.text(), [E]))
	}
}

impl Function for Euler {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
