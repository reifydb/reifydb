// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct TimeSubtract {
	info: RoutineInfo,
}

impl Default for TimeSubtract {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeSubtract {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::subtract"),
		}
	}
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl<'a> Routine<FunctionContext<'a>> for TimeSubtract {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let time_col = &args[0];
		let dur_col = &args[1];

		let (time_data, time_bv) = time_col.unwrap_option();
		let (dur_data, dur_bv) = dur_col.unwrap_option();

		match (time_data, dur_data) {
			(ColumnBuffer::Time(time_container), ColumnBuffer::Duration(dur_container)) => {
				let row_count = time_data.len();
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (time_container.get(i), dur_container.get(i)) {
						(Some(time), Some(dur)) => {
							let time_nanos = time.to_nanos_since_midnight() as i64;
							let dur_nanos =
								dur.get_nanos() + dur.get_days() as i64 * NANOS_PER_DAY;

							let result_nanos =
								(time_nanos - dur_nanos).rem_euclid(NANOS_PER_DAY);
							match Time::from_nanos_since_midnight(result_nanos as u64) {
								Some(result) => container.push(result),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				let mut result_data = ColumnBuffer::Time(container);
				if let Some(bv) = time_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				} else if let Some(bv) = dur_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Time(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}
}
