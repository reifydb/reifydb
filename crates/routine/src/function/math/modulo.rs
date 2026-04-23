// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::{Type, input_types::InputTypes};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct Modulo {
	info: FunctionInfo,
}

impl Default for Modulo {
	fn default() -> Self {
		Self::new()
	}
}

impl Modulo {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("math::mod"),
		}
	}
}

fn numeric_to_f64(data: &ColumnBuffer, i: usize) -> Option<f64> {
	match data {
		ColumnBuffer::Int1(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Int2(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Int4(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Int8(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Int16(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Uint1(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Uint2(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Uint4(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Uint8(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Uint16(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Float4(c) => c.get(i).map(|&v| v as f64),
		ColumnBuffer::Float8(c) => c.get(i).copied(),
		ColumnBuffer::Int {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		ColumnBuffer::Uint {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		ColumnBuffer::Decimal {
			container,
			..
		} => container.get(i).map(|v| v.0.to_f64().unwrap_or(0.0)),
		_ => None,
	}
}

impl Function for Modulo {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Float8
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let a_col = &args[0];
		let b_col = &args[1];

		let (a_data, a_bitvec) = a_col.data().unwrap_option();
		let (b_data, b_bitvec) = b_col.data().unwrap_option();
		let row_count = a_data.len();

		if !a_data.get_type().is_number() {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: a_data.get_type(),
			});
		}
		if !b_data.get_type().is_number() {
			return Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: b_data.get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut res_bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match (numeric_to_f64(a_data, i), numeric_to_f64(b_data, i)) {
				(Some(a), Some(b)) => {
					if b == 0.0 {
						result.push(f64::NAN);
					} else {
						result.push(a % b);
					}
					res_bitvec.push(true);
				}
				_ => {
					result.push(0.0);
					res_bitvec.push(false);
				}
			}
		}

		let result_data = ColumnBuffer::float8_with_bitvec(result, res_bitvec);
		let combined_bitvec = match (a_bitvec, b_bitvec) {
			(Some(a), Some(b)) => Some(a.and(b)),
			(Some(a), None) => Some(a.clone()),
			(None, Some(b)) => Some(b.clone()),
			(None, None) => None,
		};

		let final_data = if let Some(bv) = combined_bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv,
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
