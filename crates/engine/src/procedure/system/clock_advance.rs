// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use super::{
	super::{Procedure, context::ProcedureContext, error::ProcedureError},
	clock_set::extract_millis,
};

/// Native procedure that advances the mock clock by a number of milliseconds.
///
/// Accepts 1 positional argument: millis (any integer type).
pub struct ClockAdvanceProcedure;

impl ClockAdvanceProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for ClockAdvanceProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let millis = match ctx.params {
			Params::Positional(args) if args.len() == 1 => extract_millis("clock::advance", &args[0])?,
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("clock::advance"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("clock::advance"),
					expected: 1,
					actual: 0,
				});
			}
		};

		match &ctx.runtime_context.clock {
			Clock::Mock(mock) => {
				mock.advance_millis(millis);
				let current = mock.now_millis() as i64;
				Ok(Columns::single_row([("clock_millis", Value::Int8(current))]))
			}
			Clock::Real => Err(ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("clock::advance"),
				reason: "clock::advance can only be used with a mock clock".to_string(),
			}),
		}
	}
}
