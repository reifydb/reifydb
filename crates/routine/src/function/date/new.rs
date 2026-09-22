// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{date::Date, value_type::ValueType};

use crate::function::support::coerce::{CoerceMode, coerce_column};

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

fn ensure_integer(ctx: &FunctionContext, data: &ColumnBuffer, argument_index: usize) -> Result<(), RoutineError> {
	if !INTEGER_TYPES.contains(&data.get_type()) && data.get_type() != ValueType::Any {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: INTEGER_TYPES.to_vec(),
			actual: data.get_type(),
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

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		for i in 0..3 {
			let (data, _) = args[i].clone().split_nulls();
			ensure_integer(ctx, &data, i)?;
		}

		let year_cast = coerce_column(ctx, &args[0], ValueType::Int4, CoerceMode::Error)?;
		let month_cast = coerce_column(ctx, &args[1], ValueType::Int4, CoerceMode::Error)?;
		let day_cast = coerce_column(ctx, &args[2], ValueType::Int4, CoerceMode::Error)?;

		let (ColumnBuffer::Int4(years), ColumnBuffer::Int4(months), ColumnBuffer::Int4(days)) =
			(&year_cast, &month_cast, &day_cast)
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

		let result = ColumnBuffer::date_with_bitvec(values, bits);
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
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
