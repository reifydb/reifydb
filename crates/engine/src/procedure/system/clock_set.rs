// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use super::super::{Procedure, context::ProcedureContext, error::ProcedureError};

/// Native procedure that sets the mock clock to a specific millisecond value.
///
/// Accepts 1 positional argument: millis (any integer type).
pub struct ClockSetProcedure;

impl ClockSetProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for ClockSetProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let millis = match ctx.params {
			Params::Positional(args) if args.len() == 1 => extract_millis("clock::set", &args[0])?,
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("clock::set"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("clock::set"),
					expected: 1,
					actual: 0,
				});
			}
		};

		match &ctx.runtime_context.clock {
			Clock::Mock(mock) => {
				mock.set_millis(millis);
				let current = mock.now_millis() as i64;
				Ok(Columns::single_row([("clock_millis", Value::Int8(current))]))
			}
			Clock::Real => Err(ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("clock::set"),
				reason: "clock::set can only be used with a mock clock".to_string(),
			}),
		}
	}
}

const EXPECTED_TYPES: &[Type] = &[
	Type::Int1,
	Type::Int2,
	Type::Int4,
	Type::Int8,
	Type::Int16,
	Type::Uint1,
	Type::Uint2,
	Type::Uint4,
	Type::Uint8,
	Type::Uint16,
];

pub(crate) fn extract_millis(name: &str, value: &Value) -> Result<u64, ProcedureError> {
	match value {
		Value::Int1(v) => Ok(*v as u64),
		Value::Int2(v) => Ok(*v as u64),
		Value::Int4(v) => Ok(*v as u64),
		Value::Int8(v) => Ok(*v as u64),
		Value::Int16(v) => Ok(*v as u64),
		Value::Uint1(v) => Ok(*v as u64),
		Value::Uint2(v) => Ok(*v as u64),
		Value::Uint4(v) => Ok(*v as u64),
		Value::Uint8(v) => Ok(*v),
		Value::Uint16(v) => Ok(*v as u64),
		_ => Err(ProcedureError::InvalidArgumentType {
			procedure: Fragment::internal(name),
			argument_index: 0,
			expected: EXPECTED_TYPES.to_vec(),
			actual: value.get_type(),
		}),
	}
}
