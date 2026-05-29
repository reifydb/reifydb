// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::columns::Columns;
use reifydb_runtime::context::clock::Clock;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, datetime::DateTime, value_type::ValueType},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("clock::set"));

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::DateTime
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

		match &ctx.runtime_context.clock {
			Clock::Mock(mock) => {
				match arg {
					Value::DateTime(dt) => {
						mock.set_nanos(dt.to_nanos());
					}
					Value::Duration(dur) => {
						let epoch = DateTime::default();
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

const EXPECTED_SET_TYPES: &[ValueType] = &[
	ValueType::DateTime,
	ValueType::Duration,
	ValueType::Int1,
	ValueType::Int2,
	ValueType::Int4,
	ValueType::Int8,
	ValueType::Int16,
	ValueType::Uint1,
	ValueType::Uint2,
	ValueType::Uint4,
	ValueType::Uint8,
	ValueType::Uint16,
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
