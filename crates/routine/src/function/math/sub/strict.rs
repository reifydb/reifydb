// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::value_type::ValueType;

use crate::{
	function::{
		math::arith::{dispatch::dispatch_strict, op::Sub as SubOp},
		support::coerce::promote_pair,
	},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct SubStrict {
	info: RoutineInfo,
}

impl Default for SubStrict {
	fn default() -> Self {
		Self::new()
	}
}

impl SubStrict {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sub_strict"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for SubStrict {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		if input_types.len() >= 2 {
			promote_pair(input_types[0].clone(), input_types[1].clone())
		} else {
			input_types.first().cloned().unwrap_or(ValueType::Float8)
		}
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		dispatch_strict::<SubOp>(ctx, args)
	}
}

impl Function for SubStrict {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
