// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Round;

impl Default for Round {
	fn default() -> Self {
		Self {}
	}
}

impl Round {
	pub fn new() -> Self {
		Self::default()
	}
}

impl ScalarFunction for Round {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate at least 1 argument
		if columns.is_empty() {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		let value_column = columns.first().unwrap();

		// Get precision column if provided (default to 0)
		let precision_column = columns.get(1);

		// Helper to get precision value at row index
		let get_precision = |row_idx: usize| -> i32 {
			if let Some(prec_col) = precision_column {
				match prec_col.data() {
					ColumnData::Int4(prec_container) => {
						prec_container.get(row_idx).copied().unwrap_or(0)
					}
					ColumnData::Int1(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Int2(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Int8(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Int16(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Uint1(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Uint2(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Uint4(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Uint8(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					ColumnData::Uint16(prec_container) => {
						prec_container.get(row_idx).map(|&v| v as i32).unwrap_or(0)
					}
					_ => 0,
				}
			} else {
				0
			}
		};

		match value_column.data() {
			ColumnData::Float4(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						let precision = get_precision(row_idx);
						let multiplier = 10_f32.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::float4_with_bitvec(result, bitvec))
			}
			ColumnData::Float8(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						let precision = get_precision(row_idx);
						let multiplier = 10_f64.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::float8_with_bitvec(result, bitvec))
			}
			// Integer types: round is essentially identity (already whole numbers)
			ColumnData::Int1(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int1_with_bitvec(result, bitvec))
			}
			ColumnData::Int2(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int2_with_bitvec(result, bitvec))
			}
			ColumnData::Int4(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			ColumnData::Int8(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int8_with_bitvec(result, bitvec))
			}
			ColumnData::Int16(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::int16_with_bitvec(result, bitvec))
			}
			ColumnData::Uint1(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint1_with_bitvec(result, bitvec))
			}
			ColumnData::Uint2(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint2_with_bitvec(result, bitvec))
			}
			ColumnData::Uint4(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			ColumnData::Uint8(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint8_with_bitvec(result, bitvec))
			}
			ColumnData::Uint16(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(&value) = container.get(row_idx) {
						result.push(value);
						bitvec.push(true);
					} else {
						result.push(0);
						bitvec.push(false);
					}
				}

				Ok(ColumnData::uint16_with_bitvec(result, bitvec))
			}
			ColumnData::Int {
				container,
				max_bytes,
			} => {
				use reifydb_type::value::int::Int;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(value) = container.get(row_idx) {
						result.push(value.clone());
						bitvec.push(true);
					} else {
						result.push(Int::default());
						bitvec.push(false);
					}
				}

				Ok(ColumnData::Int {
					container: reifydb_type::value::container::number::NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Uint {
				container,
				max_bytes,
			} => {
				use reifydb_type::value::uint::Uint;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(value) = container.get(row_idx) {
						result.push(value.clone());
						bitvec.push(true);
					} else {
						result.push(Uint::default());
						bitvec.push(false);
					}
				}

				Ok(ColumnData::Uint {
					container: reifydb_type::value::container::number::NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => {
				use reifydb_type::value::decimal::Decimal;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					if let Some(value) = container.get(row_idx) {
						let prec = get_precision(row_idx);
						let f_val = value.0.to_f64().unwrap_or(0.0);
						let multiplier = 10_f64.powi(prec);
						let rounded = (f_val * multiplier).round() / multiplier;
						result.push(Decimal::from(rounded));
						bitvec.push(true);
					} else {
						result.push(Decimal::default());
						bitvec.push(false);
					}
				}

				Ok(ColumnData::Decimal {
					container: reifydb_type::value::container::number::NumberContainer::new(result),
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
}
