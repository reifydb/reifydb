// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct Gcd;

impl Gcd {
	pub fn new() -> Self {
		Self
	}
}

fn numeric_to_i64(data: &ColumnData, i: usize) -> Option<i64> {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Int8(c) => c.get(i).copied(),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as i64),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as i64),
		_ => None,
	}
}

fn compute_gcd(mut a: i64, mut b: i64) -> i64 {
	a = a.abs();
	b = b.abs();
	while b != 0 {
		let t = b;
		b = a % b;
		a = t;
	}
	a
}

impl ScalarFunction for Gcd {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let a_col = columns.get(0).unwrap();
		let b_col = columns.get(1).unwrap();

		if !a_col.data().get_type().is_number() {
			return Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
				],
				actual: a_col.data().get_type(),
			});
		}

		if !b_col.data().get_type().is_number() {
			return Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
				],
				actual: b_col.data().get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match (numeric_to_i64(a_col.data(), i), numeric_to_i64(b_col.data(), i)) {
				(Some(a), Some(b)) => {
					result.push(compute_gcd(a, b));
					bitvec.push(true);
				}
				_ => {
					result.push(0);
					bitvec.push(false);
				}
			}
		}

		Ok(ColumnData::int8_with_bitvec(result, bitvec))
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int8
	}
}
