// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{factory::date_with_bitvec, nulls::split_nulls};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	date::Date,
	value_type::ValueType,
};

use crate::function::support::coerce::{CoerceMode, bare_type, coerce_column};

pub struct DateNew {
	info: RoutineInfo,
}

impl Default for DateNew {
	fn default() -> Self {
		Self::new()
	}
}

impl DateNew {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("date::new"),
		}
	}
}

const INTEGER_TYPES: [ValueType; 10] = [
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

fn ensure_integer(ctx: &FunctionContext, data: &ColumnView, argument_index: usize) -> Result<(), RoutineError> {
	let actual = bare_type(data);
	if !INTEGER_TYPES.contains(&actual) && actual != ValueType::Any {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: INTEGER_TYPES.to_vec(),
			actual,
		});
	}
	Ok(())
}

impl<'a> Routine<FunctionContext<'a>> for DateNew {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Date
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		for (i, arg) in args.iter().enumerate().take(3) {
			let (data, _) = split_nulls(arg.clone())?;
			ensure_integer(ctx, &ColumnView::try_from(&data)?, i)?;
		}

		let year_cast =
			coerce_column(ctx, &ColumnView::try_from(&args[0])?, ValueType::Int4, CoerceMode::Error)?;
		let year_cast = ColumnView::try_from(&year_cast)?;
		let month_cast =
			coerce_column(ctx, &ColumnView::try_from(&args[1])?, ValueType::Int4, CoerceMode::Error)?;
		let month_cast = ColumnView::try_from(&month_cast)?;
		let day_cast =
			coerce_column(ctx, &ColumnView::try_from(&args[2])?, ValueType::Int4, CoerceMode::Error)?;
		let day_cast = ColumnView::try_from(&day_cast)?;

		let (ViewData::Int4(years), ViewData::Int4(months), ViewData::Int4(days)) =
			(&year_cast.data, &month_cast.data, &day_cast.data)
		else {
			unreachable!()
		};

		let row_count = years.len();
		let mut values = Vec::with_capacity(row_count);
		let mut bits = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if !year_cast.is_defined(i) || !month_cast.is_defined(i) || !day_cast.is_defined(i) {
				values.push(Date::default());
				bits.push(false);
				continue;
			}
			let y = *years.values().get(i).expect("defined row has a value");
			let m = *months.values().get(i).expect("defined row has a value");
			let d = *days.values().get(i).expect("defined row has a value");

			let date = if m >= 1 && d >= 1 {
				Date::new(y, m as u32, d as u32)
			} else {
				None
			};
			match date {
				Some(date) => {
					values.push(date);
					bits.push(true);
				}
				None => {
					return Err(RoutineError::FunctionExecutionFailed {
						function: ctx.fragment.clone(),
						reason: format!("invalid date: {}-{}-{}", y, m, d),
					});
				}
			}
		}

		Ok(date_with_bitvec(ctx.fragment.text(), values, bits))
	}
}

impl Function for DateNew {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
