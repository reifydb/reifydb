// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::sign::Signed;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::number::NumberContainer, decimal::Decimal, r#type::Type};

use crate::{
	ScalarFunction, ScalarFunctionContext,
	error::{ScalarFunctionError, ScalarFunctionResult},
	propagate_options,
};

pub struct Abs;

impl Abs {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Abs {
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

		match &column.data() {
			ColumnData::Int1(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int1_with_bitvec(data, bitvec))
			}
			ColumnData::Int2(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int2_with_bitvec(data, bitvec))
			}
			ColumnData::Int4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int4_with_bitvec(data, bitvec))
			}
			ColumnData::Int8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int8_with_bitvec(data, bitvec))
			}
			ColumnData::Int16(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int16_with_bitvec(data, bitvec))
			}
			// Unsigned integers: abs is identity (already non-negative)
			ColumnData::Uint1(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value);
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint1_with_bitvec(data, bitvec))
			}
			ColumnData::Uint2(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value);
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint2_with_bitvec(data, bitvec))
			}
			ColumnData::Uint4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value);
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint4_with_bitvec(data, bitvec))
			}
			ColumnData::Uint8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value);
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint8_with_bitvec(data, bitvec))
			}
			ColumnData::Uint16(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value);
						bitvec.push(true);
					} else {
						data.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint16_with_bitvec(data, bitvec))
			}
			ColumnData::Float4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.abs());
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
						data.push(value.abs());
						bitvec.push(true);
					} else {
						data.push(0.0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::float8_with_bitvec(data, bitvec))
			}
			ColumnData::Int {
				container,
				max_bytes,
			} => {
				use reifydb_type::value::int::Int;
				let mut data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(Int::from(value.0.clone().abs()));
					} else {
						data.push(Int::default());
					}
				}

				Ok(ColumnData::Int {
					container: NumberContainer::new(data),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Uint {
				container,
				max_bytes,
			} => {
				use reifydb_type::value::uint::Uint;
				// Unsigned: abs is identity
				let mut data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(value.clone());
					} else {
						data.push(Uint::default());
					}
				}

				Ok(ColumnData::Uint {
					container: NumberContainer::new(data),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => {
				let mut data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						data.push(Decimal::from(value.0.clone().abs()));
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
