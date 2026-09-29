// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int4_with_bitvec;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::temporal_array::datetimes,
	value_type::ValueType,
};

pub struct DateTimeQuarter {
	info: RoutineInfo,
}

impl Default for DateTimeQuarter {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeQuarter {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::quarter"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeQuarter {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int4
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		let result_data = match &data.data {
			ViewData::DateTime(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(dt) = datetimes(container).get(i) {
						let date = dt.date();
						let quarter = (date.month() as i32 - 1) / 3 + 1;
						result.push(quarter);
						res_bitvec.push(true);
					} else {
						result.push(0);
						res_bitvec.push(false);
					}
				}

				int4_with_bitvec(ctx.fragment.text(), result, res_bitvec)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![ValueType::DateTime],
					actual: data.get_type(),
				});
			}
		};

		Ok(result_data)
	}
}

impl Function for DateTimeQuarter {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
