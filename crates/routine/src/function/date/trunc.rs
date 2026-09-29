// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{
		temporal_array::{date_array, dates},
		varlen_array,
	},
	date::Date,
	value_type::ValueType,
};

use crate::function::support::column::array_column;

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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let date_data = ColumnView::try_from(&args[0])?;
		let prec_data = ColumnView::try_from(&args[1])?;
		let row_count = date_data.len();

		let result_data = match (&date_data.data, &prec_data.data) {
			(
				ViewData::Date(date_container),
				ViewData::Utf8 {
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

				array_column(ctx.fragment.text(), ValueType::Date, Arc::new(date_array(container)))
			}
			(ViewData::Date(_), _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![ValueType::Utf8],
					actual: prec_data.get_type(),
				});
			}
			(_, _) => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::Date],
					actual: date_data.get_type(),
				});
			}
		};

		Ok(result_data)
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
