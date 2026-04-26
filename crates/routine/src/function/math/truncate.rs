// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{
	container::number::NumberContainer,
	decimal::Decimal,
	r#type::{Type, input_types::InputTypes},
};

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct Truncate {
	info: RoutineInfo,
}

impl Default for Truncate {
	fn default() -> Self {
		Self::new()
	}
}

impl Truncate {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::truncate"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Truncate {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
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

		let result_data = match data {
			ColumnBuffer::Float4(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.trunc());
						res_bitvec.push(true);
					} else {
						data.push(0.0);
						res_bitvec.push(false);
					}
				}
				ColumnBuffer::float4_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Float8(container) => {
				let mut data = Vec::with_capacity(row_count);
				let mut res_bitvec = Vec::with_capacity(row_count);
				for i in 0..row_count {
					if let Some(&value) = container.get(i) {
						data.push(value.trunc());
						res_bitvec.push(true);
					} else {
						data.push(0.0);
						res_bitvec.push(false);
					}
				}
				ColumnBuffer::float8_with_bitvec(data, res_bitvec)
			}
			ColumnBuffer::Decimal {
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
				ColumnBuffer::Decimal {
					container: NumberContainer::new(data),
					precision: *precision,
					scale: *scale,
				}
			}
			other if other.get_type().is_number() => data.clone(),
			other => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.env.fragment.clone(),
					argument_index: 0,
					expected: InputTypes::numeric().expected_at(0).to_vec(),
					actual: other.get_type(),
				});
			}
		};

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
