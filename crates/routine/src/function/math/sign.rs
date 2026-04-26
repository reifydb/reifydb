// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::{Type, input_types::InputTypes};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Sign {
	info: RoutineInfo,
}

impl Default for Sign {
	fn default() -> Self {
		Self::new()
	}
}

impl Sign {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sign"),
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

impl<'a> Routine<FunctionContext<'a>> for Sign {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Int4
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		if !data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: data.get_type(),
			});
		}

		let mut result = Vec::with_capacity(row_count);
		let mut res_bitvec = Vec::with_capacity(row_count);

		for i in 0..row_count {
			match numeric_to_f64(data, i) {
				Some(v) => {
					let sign = if v > 0.0 {
						1i32
					} else if v < 0.0 {
						-1i32
					} else {
						0i32
					};
					result.push(sign);
					res_bitvec.push(true);
				}
				None => {
					result.push(0);
					res_bitvec.push(false);
				}
			}
		}

		let result_data = ColumnBuffer::int4_with_bitvec(result, res_bitvec);
		let final_data = if let Some(bv) = bitvec {
			ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			}
		} else {
			result_data
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
	}
}
