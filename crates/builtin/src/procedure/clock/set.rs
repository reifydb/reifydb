// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};
use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, datetime::DateTime, r#type::Type},
};

/// Native procedure that sets the mock clock to a specific time.
///
/// Accepts 1 positional argument: a DateTime, Duration (since epoch), or integer milliseconds.
pub struct ClockSetProcedure;

impl ClockSetProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for ClockSetProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let arg = match ctx.params {
			Params::Positional(args) if args.len() == 1 => &args[0],
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
				match arg {
					Value::DateTime(dt) => {
						mock.set_nanos(dt.to_nanos_since_epoch_u128());
					}
					Value::Duration(dur) => {
						let epoch = DateTime::default(); // 1970-01-01T00:00:00Z
						let target = epoch.add_duration(dur)?;
						mock.set_nanos(target.to_nanos_since_epoch_u128());
					}
					other => {
						let millis = extract_millis(other).ok_or_else(|| {
							ProcedureError::InvalidArgumentType {
								procedure: Fragment::internal("clock::set"),
								argument_index: 0,
								expected: EXPECTED_SET_TYPES.to_vec(),
								actual: other.get_type(),
							}
						})?;
						mock.set_millis(millis);
					}
				}
				let current_nanos = mock.now_nanos();
				let dt = DateTime::from_timestamp_nanos(current_nanos)?;
				Ok(Columns::single_row([("clock", Value::DateTime(dt))]))
			}
			Clock::Real => Err(ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("clock::set"),
				reason: "clock::set can only be used with a mock clock".to_string(),
			}),
		}
	}
}

const EXPECTED_SET_TYPES: &[Type] = &[
	Type::DateTime,
	Type::Duration,
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

pub fn extract_millis(value: &Value) -> Option<u64> {
	match value {
		Value::Int1(v) => Some(*v as u64),
		Value::Int2(v) => Some(*v as u64),
		Value::Int4(v) => Some(*v as u64),
		Value::Int8(v) => Some(*v as u64),
		Value::Int16(v) => Some(*v as u64),
		Value::Uint1(v) => Some(*v as u64),
		Value::Uint2(v) => Some(*v as u64),
		Value::Uint4(v) => Some(*v as u64),
		Value::Uint8(v) => Some(*v),
		Value::Uint16(v) => Some(*v as u64),
		_ => None,
	}
}
