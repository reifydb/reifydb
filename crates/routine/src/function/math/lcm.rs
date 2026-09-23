// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::value_type::ValueType;

pub struct Lcm {
	info: RoutineInfo,
}

impl Default for Lcm {
	fn default() -> Self {
		Self::new()
	}
}

impl Lcm {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::lcm"),
		}
	}
}

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

fn numeric_to_i128(data: &ColumnBuffer, i: usize) -> Option<i128> {
	match data {
		ColumnBuffer::Int1(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Int2(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Int4(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Int8(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Int16(c) => c.values().get(i).copied(),
		ColumnBuffer::Uint1(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Uint2(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Uint4(c) => c.values().get(i).map(|&v| v as i128),
		ColumnBuffer::Uint8(c) => c.values().get(i).map(|&v| v as i128),
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

fn compute_lcm(a: i128, b: i128) -> Option<u128> {
	if a == 0 || b == 0 {
		return Some(0);
	}
	let a = a.unsigned_abs();
	let b = b.unsigned_abs();
	(a / compute_gcd(a, b)).checked_mul(b)
}

impl<'a> Routine<FunctionContext<'a>> for Lcm {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Int8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let a_data = &args[0];
		let b_data = &args[1];
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
			match (numeric_to_i128(a_data, i), numeric_to_i128(b_data, i)) {
				(Some(a), Some(b)) => {
					let multiple = compute_lcm(a, b)
						.and_then(|multiple| i64::try_from(multiple).ok())
						.ok_or_else(|| {
							failed(
								ctx,
								format!("the least common multiple of {a} and {b} is out of range for {}", ValueType::Int8),
							)
						})?;
					result.push(multiple);
					res_bitvec.push(true);
				}
				_ => {
					result.push(0);
					res_bitvec.push(false);
				}
			}
		}

		let result_data = ColumnBuffer::int8_with_bitvec(result, res_bitvec);

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result_data)]))
	}
}

impl Function for Lcm {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(2)
	}
}
