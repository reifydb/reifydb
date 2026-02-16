// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Cos;

impl Cos {
	pub fn new() -> Self {
		Self
	}
}

fn numeric_to_f64(data: &ColumnData, i: usize) -> Option<f64> {
	match data {
		ColumnData::Int1(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Int2(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Int4(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Int8(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Int16(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Uint1(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Uint2(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Uint4(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Uint8(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Uint16(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Float4(c) => c.get(i).map(|&v| v as f64),
		ColumnData::Float8(c) => c.get(i).copied(),
		ColumnData::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		ColumnData::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		ColumnData::Decimal {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		_ => None,
	}
}

impl ScalarFunction for Cos {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 1 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: columns.len(),
			});
		}

		let column = columns.get(0).unwrap();

		if !column.data().get_type().is_number() {
			return Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![
					Type::Int1,
					Type::Int2,
					Type::Int4,
					Type::Int8,
					Type::Int16,
					Type::Uint1,
					Type::Uint2,
					Type::Uint4,
					Type::Uint8,
					Type::Uint16,
					Type::Float4,
					Type::Float8,
					Type::Int,
					Type::Uint,
					Type::Decimal,
				],
				actual: column.data().get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match numeric_to_f64(column.data(), i) {
				Some(v) => {
					result.push(v.cos());
					bitvec.push(true);
				}
				None => {
					result.push(0.0);
					bitvec.push(false);
				}
			}
		}

		Ok(ColumnData::float8_with_bitvec(result, bitvec))
	}
}
