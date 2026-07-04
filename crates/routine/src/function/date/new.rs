// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{container::number::NumberContainer, date::Date, value_type::ValueType},
};

use crate::{
	function::support::coerce::{CoercePolicy, coerce_column},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

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

fn defined(c: &NumberContainer<i32>, bv: Option<&BitVec>, i: usize) -> bool {
	c.is_defined(i) && bv.is_none_or(|b| b.get(i))
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
		if args.len() != 3 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 3,
				actual: args.len(),
			});
		}

		for i in 0..3 {
			let (data, _) = args[i].unwrap_option();
			ensure_integer(ctx, data, i)?;
		}

		let year_cast = coerce_column(ctx, &args[0], ValueType::Int4, CoercePolicy::Error)?;
		let month_cast = coerce_column(ctx, &args[1], ValueType::Int4, CoercePolicy::Error)?;
		let day_cast = coerce_column(ctx, &args[2], ValueType::Int4, CoercePolicy::Error)?;

		let (year_inner, year_bv) = year_cast.unwrap_option();
		let (month_inner, month_bv) = month_cast.unwrap_option();
		let (day_inner, day_bv) = day_cast.unwrap_option();
		let (ColumnBuffer::Int4(years), ColumnBuffer::Int4(months), ColumnBuffer::Int4(days)) =
			(year_inner, month_inner, day_inner)
		else {
			unreachable!()
		};

		let row_count = years.len();
		let mut values = Vec::with_capacity(row_count);
		let mut bits = Vec::with_capacity(row_count);

		for i in 0..row_count {
			if !defined(years, year_bv, i) || !defined(months, month_bv, i) || !defined(days, day_bv, i) {
				values.push(Date::default());
				bits.push(false);
				continue;
			}
			let y = *years.get(i).expect("defined row has a value");
			let m = *months.get(i).expect("defined row has a value");
			let d = *days.get(i).expect("defined row has a value");

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
}
