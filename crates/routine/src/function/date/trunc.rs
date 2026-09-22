// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	container::{
		temporal_array::{date_array, dates},
		varlen_array,
	},
	date::Date,
	value_type::ValueType,
};

pub struct DateTrunc {
	info: RoutineInfo,
}

impl Default for DateTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTrunc {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::trunc"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTrunc {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Date
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let date_col = &args[0];
		let prec_col = &args[1];
		let (date_data, date_bitvec) = date_col.unwrap_option();
		let (prec_data, prec_bitvec) = prec_col.unwrap_option();
		let row_count = date_data.len();

		let result_data = match (date_data, prec_data) {
			(
				ColumnBuffer::Date(date_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = Vec::with_capacity(row_count);

				for i in 0..row_count {
					match (dates(date_container).get(i), varlen_array::get(prec_container, i)) {
						(Some(d), Some(precision)) => {
							let truncated =
								match precision {
									"year" => Date::new(d.year(), 1, 1),
									"month" => Date::new(d.year(), d.month(), 1),
									other => {
										return Err(RoutineError::FunctionExecutionFailed {
										function: ctx.fragment.clone(),
										reason: format!("invalid precision: '{}'", other),
									});
									}
								};
							match truncated {
								Some(val) => container.push(val),
								None => container.push(Date::default()),
							}
						}
						_ => container.push(Date::default()),
					}
				}

				ColumnBuffer::Date(date_array(container))
			}
			(ColumnBuffer::Date(_), other) => {
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
					expected: vec![ValueType::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (date_bitvec, prec_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for DateTrunc {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
