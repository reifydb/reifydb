// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::PI;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct Pi {
	info: RoutineInfo,
}

impl Default for Pi {
	fn default() -> Self {
		Self::new()
	}
}

impl Pi {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::pi"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Pi {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Float8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, _args: &Columns) -> Result<Columns, RoutineError> {
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), ColumnBuffer::float8(vec![PI]))]))
	}
}

impl Function for Pi {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(0)
	}
}
