// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::datetime;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	datetime::DateTime,
	value_type::ValueType,
};

use crate::function::support::coerce::read_i64;

pub struct DateTimeFromEpochMillis {
	info: RoutineInfo,
}

impl Default for DateTimeFromEpochMillis {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTimeFromEpochMillis {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("datetime::from_epoch_millis"),
		}
	}
}

fn is_integer_type(data: &ColumnView) -> bool {
	matches!(
		&data.data,
		ViewData::Int1(_)
			| ViewData::Int2(_)
			| ViewData::Int4(_)
			| ViewData::Int8(_)
			| ViewData::Int16(_)
			| ViewData::Uint1(_)
			| ViewData::Uint2(_)
			| ViewData::Uint4(_)
			| ViewData::Uint8(_)
			| ViewData::Uint16(_)
	)
}

impl<'a> Routine<FunctionContext<'a>> for DateTimeFromEpochMillis {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::DateTime
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		if !is_integer_type(&data) {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
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
				],
				actual: data.get_type(),
			});
		}

		let mut container = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if let Some(millis) = read_i64(&ctx.fragment, &data, i)? {
				container.push(DateTime::from_epoch_millis(millis)?);
			} else {
				container.push(DateTime::default());
			}
		}

		let result_data = datetime(ctx.fragment.text(), container);

		Ok(result_data)
	}
}

impl Function for DateTimeFromEpochMillis {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
