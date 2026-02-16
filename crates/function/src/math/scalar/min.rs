// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct Min;

impl Min {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for Min {
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

		// For min function, we need to find the minimum value across all columns for each row
		let first_column = columns.get(0).unwrap();

		match first_column.data() {
			ColumnData::Int1(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i8> = None;

					// Check all columns for this row
					for column in columns.iter() {
						if let ColumnData::Int1(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int1_with_bitvec(result, bitvec))
			}
			ColumnData::Int2(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i16> = None;

					for column in columns.iter() {
						if let ColumnData::Int2(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int2_with_bitvec(result, bitvec))
			}
			ColumnData::Int4(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i32> = None;

					for column in columns.iter() {
						if let ColumnData::Int4(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int4_with_bitvec(result, bitvec))
			}
			ColumnData::Int8(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i64> = None;

					for column in columns.iter() {
						if let ColumnData::Int8(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int8_with_bitvec(result, bitvec))
			}
			ColumnData::Int16(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<i128> = None;

					for column in columns.iter() {
						if let ColumnData::Int16(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::int16_with_bitvec(result, bitvec))
			}
			ColumnData::Uint1(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<u8> = None;

					for column in columns.iter() {
						if let ColumnData::Uint1(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint1_with_bitvec(result, bitvec))
			}
			ColumnData::Uint2(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<u16> = None;

					for column in columns.iter() {
						if let ColumnData::Uint2(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint2_with_bitvec(result, bitvec))
			}
			ColumnData::Uint4(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<u32> = None;

					for column in columns.iter() {
						if let ColumnData::Uint4(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint4_with_bitvec(result, bitvec))
			}
			ColumnData::Uint8(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<u64> = None;

					for column in columns.iter() {
						if let ColumnData::Uint8(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint8_with_bitvec(result, bitvec))
			}
			ColumnData::Uint16(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<u128> = None;

					for column in columns.iter() {
						if let ColumnData::Uint16(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::uint16_with_bitvec(result, bitvec))
			}
			ColumnData::Float4(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<f32> = None;

					for column in columns.iter() {
						if let ColumnData::Float4(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(0.0);
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::float4_with_bitvec(result, bitvec))
			}
			ColumnData::Float8(_) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<f64> = None;

					for column in columns.iter() {
						if let ColumnData::Float8(container) = column.data() {
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => *value,
									Some(current_min) => current_min.min(*value),
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
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
			ColumnData::Int {
				max_bytes,
				..
			} => {
				use reifydb_type::value::int::Int;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<Int> = None;

					for column in columns.iter() {
						if let ColumnData::Int {
							container,
							..
						} = column.data()
						{
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => value.clone(),
									Some(current_min) => {
										if value < &current_min {
											value.clone()
										} else {
											current_min
										}
									}
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(Int::default());
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::Int {
					container: reifydb_type::value::container::number::NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Uint {
				max_bytes,
				..
			} => {
				use reifydb_type::value::uint::Uint;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<Uint> = None;

					for column in columns.iter() {
						if let ColumnData::Uint {
							container,
							..
						} = column.data()
						{
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => value.clone(),
									Some(current_min) => {
										if value < &current_min {
											value.clone()
										} else {
											current_min
										}
									}
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(Uint::default());
							bitvec.push(false);
						}
					}
				}

				Ok(ColumnData::Uint {
					container: reifydb_type::value::container::number::NumberContainer::new(result),
					max_bytes: *max_bytes,
				})
			}
			ColumnData::Decimal {
				precision,
				scale,
				..
			} => {
				use reifydb_type::value::decimal::Decimal;
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);

				for row_idx in 0..row_count {
					let mut min_value: Option<Decimal> = None;

					for column in columns.iter() {
						if let ColumnData::Decimal {
							container,
							..
						} = column.data()
						{
							if let Some(value) = container.get(row_idx) {
								min_value = Some(match min_value {
									None => value.clone(),
									Some(current_min) => {
										if value < &current_min {
											value.clone()
										} else {
											current_min
										}
									}
								});
							}
						}
					}

					match min_value {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push(Decimal::default());
							bitvec.push(false);
						}
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

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types[0].clone()
	}
}
