// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{
	container::number::NumberContainer,
	decimal::Decimal,
	r#type::{Type, input_types::InputTypes},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Round {
	info: FunctionInfo,
}

impl Default for Round {
	fn default() -> Self {
		Self::new()
	}
}

impl Round {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::round"),
		}
	}
}

impl Function for Round {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: 0,
			});
		}

		let value_column = &args[0];
		let precision_column = args.get(1);

		let (val_data, val_bitvec) = value_column.data().unwrap_option();
		let row_count = val_data.len();

		// Helper to get precision value at row index
		let get_precision = |row_idx: usize| -> i32 {
			if let Some(prec_col) = precision_column {
				let (p_data, _) = prec_col.data().unwrap_option();
				match p_data {
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

		let result_data = match val_data {
			ColumnData::Float4(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						let precision = get_precision(i);
						let multiplier = 10_f32.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}
				ColumnData::float4_with_bitvec(result, bitvec)
			}
			ColumnData::Float8(container) => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						let precision = get_precision(i);
						let multiplier = 10_f64.powi(precision);
						let rounded = (value * multiplier).round() / multiplier;
						result.push(rounded);
						bitvec.push(true);
					} else {
						result.push(0.0);
						bitvec.push(false);
					}
				}
				ColumnData::float8_with_bitvec(result, bitvec)
			}
			ColumnData::Decimal {
				container,
				precision,
				scale,
			} => {
				let mut result = Vec::with_capacity(row_count);
				let mut bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(value) = container.get(i) {
						let prec = get_precision(i);
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
				ColumnData::Decimal {
					container: NumberContainer::new(result),
					precision: *precision,
					scale: *scale,
				}
			}
			other if other.get_type().is_number() => val_data.clone(),
			other => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		};

		let final_data = if let Some(bv) = val_bitvec {
			ColumnData::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
