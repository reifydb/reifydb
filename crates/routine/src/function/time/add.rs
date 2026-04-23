// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, time::Time, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TimeAdd {
	info: FunctionInfo,
}

impl Default for TimeAdd {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeAdd {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("time::add"),
		}
	}
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl Function for TimeAdd {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let time_col = &args[0];
		let dur_col = &args[1];

		let (time_data, time_bv) = time_col.data().unwrap_option();
		let (dur_data, dur_bv) = dur_col.data().unwrap_option();

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
								(time_nanos + dur_nanos).rem_euclid(NANOS_PER_DAY);
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
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Time(_), other) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Duration],
				actual: other.get_type(),
			}),
			(other, _) => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
		}
	}
}
