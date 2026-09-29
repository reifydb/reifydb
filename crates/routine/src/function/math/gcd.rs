// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::int8_with_bitvec;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::wide_int_array::wide_at,
	value_type::ValueType,
};

pub struct Gcd {
	info: RoutineInfo,
}

impl Default for Gcd {
	fn default() -> Self {
		Self::new()
	}
}

impl Gcd {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::gcd"),
		}
	}
}

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

fn numeric_to_i128(data: &ColumnView, i: usize) -> Option<i128> {
	match &data.data {
		ViewData::Int1(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Int2(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Int4(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Int8(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Int16(c) => wide_at::<i128>(c, i),
		ViewData::Uint1(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Uint2(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Uint4(c) => c.values().get(i).map(|&v| v as i128),
		ViewData::Uint8(c) => c.values().get(i).map(|&v| v as i128),
		_ => None,
	}
}

fn compute_gcd(mut a: u128, mut b: u128) -> u128 {
	while b != 0 {
		let t = b;
		b = a % b;
		a = t;
	}
	a
}

impl<'a> Routine<FunctionContext<'a>> for Gcd {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let a_data = ColumnView::try_from(&args[0])?;
		let b_data = ColumnView::try_from(&args[1])?;
		let row_count = a_data.len();

		let expected_types = vec![
			ValueType::Int1,
			ValueType::Int2,
			ValueType::Int4,
			ValueType::Int8,
			ValueType::Uint1,
			ValueType::Uint2,
			ValueType::Uint4,
			ValueType::Uint8,
		];
		if !a_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: expected_types,
				actual: a_data.get_type(),
			});
		}
		if !b_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: expected_types,
				actual: b_data.get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut res_bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match (numeric_to_i128(&a_data, i), numeric_to_i128(&b_data, i)) {
				(Some(a), Some(b)) => {
					let divisor = compute_gcd(a.unsigned_abs(), b.unsigned_abs());
					result.push(i64::try_from(divisor).map_err(|_| {
						failed(
							ctx,
							format!(
								"the greatest common divisor of {a} and {b} is out of range for {}",
								ValueType::Int8
							),
						)
					})?);
					res_bitvec.push(true);
				}
				_ => {
					result.push(0);
					res_bitvec.push(false);
				}
			}
		}

		Ok(int8_with_bitvec(ctx.fragment.text(), result, res_bitvec))
	}
}

impl Function for Gcd {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
