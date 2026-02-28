// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct Truncate;

impl Truncate {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Truncate {
	fn scalar(&self, ctx: ScalarFunctionContext) -> ScalarFunctionResult<ColumnData> {
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

		match column.data() {
			ColumnData::Float4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.trunc());
						bitvec.push(true);
					} else {
						data.push(0.0);
						bitvec.push(false);
					}
				}
				Ok(ColumnData::float4_with_bitvec(data, bitvec))
			}
			ColumnData::Float8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.trunc());
						bitvec.push(true);
					} else {
						data.push(0.0);
						bitvec.push(false);
					}
				}
				Ok(ColumnData::float8_with_bitvec(data, bitvec))
			}
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => {
				let mut data = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						let f = value.0.to_f64().unwrap_or(0.0);
						data.push(Decimal::from(f.trunc()));
					} else {
						data.push(Decimal::default());
					}
				}
				Ok(ColumnData::Decimal {
					container: NumberContainer::new(data),
					precision: *precision,
					scale: *scale,
				})
			}
			other if other.get_type().is_number() => Ok(column.data().clone()),
			other => Err(ScalarFunctionError::InvalidArgumentType {
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
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types[0].clone()
	}
}
