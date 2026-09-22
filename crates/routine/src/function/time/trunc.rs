// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::{
		temporal_array::{time_array, times},
		varlen_array::get,
	},
	time::Time,
	value_type::ValueType,
};

pub struct TimeTrunc {
	info: RoutineInfo,
}

impl Default for TimeTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("time::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TimeTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Time
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let time_col = &args[0];
		let prec_col = &args[1];

		let (time_data, time_bv) = time_col.unwrap_option();
		let (prec_data, _) = prec_col.unwrap_option();

		match (time_data, prec_data) {
			(
				ColumnBuffer::Time(time_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let row_count = time_data.len();
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (times(time_container).get(i), get(prec_container, i).is_some()) {
						(Some(t), true) => {
							let precision = get(prec_container, i).unwrap();
							let truncated = match precision {
								"hour" => Time::new(t.hour(), 0, 0, 0),
								"minute" => Time::new(t.hour(), t.minute(), 0, 0),
								"second" => {
									Time::new(t.hour(), t.minute(), t.second(), 0)
								}
								other => {
									return Err(
										RoutineError::FunctionExecutionFailed {
											function: ctx.fragment.clone(),
											reason: format!(
												"invalid precision: '{}'",
												other
											),
										},
									);
								}
							};
							match truncated {
								Some(val) => container.push(val),
								None => container.push(Time::default()),
							}
						}
						_ => container.push(Time::default()),
					}
				}

				let mut result_data = ColumnBuffer::Time(time_array(container));
				if let Some(bv) = time_bv {
					result_data = ColumnBuffer::Option {
						inner: Box::new(result_data),
						bitvec: bv.clone(),
					};
				}
				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
			}
			(ColumnBuffer::Time(_), other) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
			(other, _) => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Time],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for TimeTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
