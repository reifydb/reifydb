// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, ScalarFunctionError};

pub struct Power;

impl Power {
	pub fn new() -> Self {
		Self {}
	}
}

impl ScalarFunction for Power {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::ScalarFunctionResult<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		// Validate exactly 2 arguments
		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let base_column = columns.get(0).unwrap();
		let exponent_column = columns.get(1).unwrap();

		match (base_column.data(), exponent_column.data()) {
			(ColumnData::Int1(base_container), ColumnData::Int1(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0 // Integer power with negative exponent results in 0
							} else {
								(base_val as i32).pow(exp_val as u32)
							};
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			(ColumnData::Int2(base_container), ColumnData::Int2(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								(base_val as i32).pow(exp_val as u32)
							};
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			(ColumnData::Int4(base_container), ColumnData::Int4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			(ColumnData::Int8(base_container), ColumnData::Int8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
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
			(ColumnData::Int16(base_container), ColumnData::Int16(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = if exp_val < 0 {
								0
							} else {
								base_val.saturating_pow(exp_val as u32)
							};
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int16_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint1(base_container), ColumnData::Uint1(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result =
								(base_val as u32).saturating_pow(exp_val as u32);
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint2(base_container), ColumnData::Uint2(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result =
								(base_val as u32).saturating_pow(exp_val as u32);
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint4(base_container), ColumnData::Uint4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val);
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint8(base_container), ColumnData::Uint8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val as u32);
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint8_with_bitvec(result, bitvec))
			}
			(ColumnData::Uint16(base_container), ColumnData::Uint16(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							let power_result = base_val.saturating_pow(exp_val as u32);
							result.push(power_result);
							bitvec.push(true);
						}
						_ => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint16_with_bitvec(result, bitvec))
			}
			(ColumnData::Float4(base_container), ColumnData::Float4(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							result.push(base_val.powf(exp_val));
							bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::float4_with_bitvec(result, bitvec))
			}
			(ColumnData::Float8(base_container), ColumnData::Float8(exp_container)) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(&base_val), Some(&exp_val)) => {
							result.push(base_val.powf(exp_val));
							bitvec.push(true);
						}
						_ => {
							result.push(0.0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::float8_with_bitvec(result, bitvec))
			}
			(
				ColumnData::Int {
					container: base_container,
					max_bytes,
				},
				ColumnData::Int {
					container: exp_container,
					..
				},
			) => {
				use reifydb_type::value::int::Int;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);
							result.push(Int::from(b.powf(e) as i64));
							bitvec.push(true);
						}
						_ => {
							result.push(Int::default());
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::Int {
					container: reifydb_type::value::container::number::NumberContainer::new(
						result,
						bitvec.into(),
					),
					max_bytes: *max_bytes,
				})
			}
			(
				ColumnData::Uint {
					container: base_container,
					max_bytes,
				},
				ColumnData::Uint {
					container: exp_container,
					..
				},
			) => {
				use reifydb_type::value::uint::Uint;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);
							result.push(Uint::from(b.powf(e) as u64));
							bitvec.push(true);
						}
						_ => {
							result.push(Uint::default());
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::Uint {
					container: reifydb_type::value::container::number::NumberContainer::new(
						result,
						bitvec.into(),
					),
					max_bytes: *max_bytes,
				})
			}
			(
				ColumnData::Decimal {
					container: base_container,
					precision,
					scale,
				},
				ColumnData::Decimal {
					container: exp_container,
					..
				},
			) => {
				use reifydb_type::value::decimal::Decimal;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let base = base_container.get(row_idx);
					let exp = exp_container.get(row_idx);

					match (base, exp) {
						(Some(base_val), Some(exp_val)) => {
							let b = base_val.0.to_f64().unwrap_or(0.0);
							let e = exp_val.0.to_f64().unwrap_or(0.0);
							result.push(Decimal::from(b.powf(e)));
							bitvec.push(true);
						}
						_ => {
							result.push(Decimal::default());
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::Decimal {
					container: reifydb_type::value::container::number::NumberContainer::new(
						result,
						bitvec.into(),
					),
					precision: *precision,
					scale: *scale,
				})
			}
			_ => Err(ScalarFunctionError::InvalidArgumentType {
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
				actual: base_column.data().get_type(),
			}),
		}
	}
}
