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

use super::set::extract_millis;
use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("clock::advance"));

pub struct ClockAdvanceProcedure;

impl Default for ClockAdvanceProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl ClockAdvanceProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for ClockAdvanceProcedure {
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
					procedure: Fragment::internal("clock::advance"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("clock::advance"),
					expected: 1,
					actual: 0,
				});
			}
		};

		match &ctx.runtime_context.clock {
			Clock::Mock(mock) => {
				match arg {
					Value::Duration(dur) => {
						if dur.get_months() == 0 && dur.get_days() == 0 {
							let nanos = dur.get_nanos();
							if nanos >= 0 {
								mock.advance_nanos(nanos as u64);
							} else {
								let current = mock.now_nanos();
								let abs_nanos = nanos.unsigned_abs();
								if abs_nanos > current {
									return Err(RoutineError::ProcedureExecutionFailed {
										procedure: Fragment::internal("clock::advance"),
										reason: "clock cannot be set before Unix epoch".to_string(),
									});
								}
								mock.set_nanos(current - abs_nanos);
							}
						} else {
							let current_nanos = mock.now_nanos();
							let current_dt = DateTime::from_nanos(current_nanos);
							let new_dt = current_dt.add_duration(dur)?;
							mock.set_nanos(new_dt.to_nanos());
						}
					}
					other => {
						let millis = extract_millis(other).ok_or_else(|| {
							RoutineError::ProcedureInvalidArgumentType {
								procedure: Fragment::internal("clock::advance"),
								argument_index: 0,
								expected: EXPECTED_ADVANCE_TYPES.to_vec(),
								actual: other.get_type(),
							}
						})?;
						mock.advance_millis(millis);
					}
				}
				let current_nanos = mock.now_nanos();
				let dt = DateTime::from_nanos(current_nanos);
				Ok(Columns::single_row([("clock", Value::DateTime(dt))]))
			}
			Clock::Real => Err(RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("clock::advance"),
				reason: "clock::advance can only be used with a mock clock".to_string(),
			}),
		}
	}
}

const EXPECTED_ADVANCE_TYPES: &[Type] = &[
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
