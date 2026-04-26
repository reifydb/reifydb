// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, datetime::DateTime, r#type::Type},
};

use crate::routine::{ProcedureContext, Routine, RoutineError, RoutineInfo};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("clock::set"));

/// Native procedure that sets the mock clock to a specific time.
///
/// Accepts 1 positional argument: a DateTime, Duration (since epoch), or integer milliseconds.
pub struct ClockSetProcedure;

impl Default for ClockSetProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl ClockSetProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for ClockSetProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let arg = match ctx.params {
			Params::Positional(args) if args.len() == 1 => &args[0],
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("clock::set"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("clock::set"),
					expected: 1,
					actual: 0,
				});
			}
		};

		match &ctx.env.runtime_context.clock {
			Clock::Mock(mock) => {
				match arg {
					Value::DateTime(dt) => {
						mock.set_nanos(dt.to_nanos());
					}
					Value::Duration(dur) => {
						let epoch = DateTime::default(); // 1970-01-01T00:00:00Z
						let target = epoch.add_duration(dur)?;
						mock.set_nanos(target.to_nanos());
					}
					other => {
						let millis = extract_millis(other).ok_or_else(|| {
							RoutineError::ProcedureInvalidArgumentType {
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
				let dt = DateTime::from_nanos(current_nanos);
				Ok(Columns::single_row([("clock", Value::DateTime(dt))]))
			}
			Clock::Real => Err(RoutineError::ProcedureExecutionFailed {
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
