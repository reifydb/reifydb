// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::{
		temporal_array::{datetime_array, datetimes},
		varlen_array::get,
	},
	datetime::DateTime,
	value_type::ValueType,
};

pub struct DateTimeTrunc {
	info: RoutineInfo,
}

impl Default for DateTimeTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::DateTime
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let dt_col = &args[0];
		let prec_col = &args[1];
		let (dt_data, dt_bitvec) = dt_col.unwrap_option();
		let (prec_data, prec_bitvec) = prec_col.unwrap_option();
		let row_count = dt_data.len();

		let result_data = match (dt_data, prec_data) {
			(
				ColumnBuffer::DateTime(dt_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (datetimes(dt_container).get(i), get(prec_container, i).is_some()) {
						(Some(dt), true) => {
							let precision = get(prec_container, i).unwrap();
							let truncated = match precision {
								"year" => DateTime::new(dt.year(), 1, 1, 0, 0, 0, 0),
								"month" => DateTime::new(
									dt.year(),
									dt.month(),
									1,
									0,
									0,
									0,
									0,
								),
								"day" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									0,
									0,
									0,
									0,
								),
								"hour" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									0,
									0,
									0,
								),
								"minute" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									dt.minute(),
									0,
									0,
								),
								"second" => DateTime::new(
									dt.year(),
									dt.month(),
									dt.day(),
									dt.hour(),
									dt.minute(),
									dt.second(),
									0,
								),
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
								None => container.push(DateTime::default()),
							}
						}
						_ => container.push(DateTime::default()),
					}
				}

				ColumnBuffer::DateTime(datetime_array(container))
			}
			(ColumnBuffer::DateTime(_), other) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::Utf8],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (dt_bitvec, prec_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateTimeTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
