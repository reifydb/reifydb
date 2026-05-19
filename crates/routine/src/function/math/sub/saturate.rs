// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::r#type::{Type, input_types::InputTypes};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct SubSaturate {
	info: RoutineInfo,
}

impl Default for SubSaturate {
	fn default() -> Self {
		Self::new()
	}
}

impl SubSaturate {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::sub_saturate"),
		}
	}
}

macro_rules! sub_sat_int {
	($a:ident, $b:ident, $row_count:expr, $factory:ident) => {{
		let mut result = Vec::with_capacity($row_count);
		let mut bitvec = Vec::with_capacity($row_count);
		for i in 0..$row_count {
			match ($a.get(i), $b.get(i)) {
				(Some(&l), Some(&r)) => {
					result.push(l.saturating_sub(r));
					bitvec.push(true);
				}
				_ => {
					result.push(0);
					bitvec.push(false);
				}
			}
		}
		ColumnBuffer::$factory(result, bitvec)
	}};
}

macro_rules! sub_sat_signed {
	($a:ident, $b:ident, $row_count:expr, $factory:ident, $zero:expr) => {{
		let mut result = Vec::with_capacity($row_count);
		let mut bitvec = Vec::with_capacity($row_count);
		for i in 0..$row_count {
			match ($a.get(i), $b.get(i)) {
				(Some(&l), Some(&r)) => {
					let diff = l.saturating_sub(r);
					result.push(if diff < $zero {
						$zero
					} else {
						diff
					});
					bitvec.push(true);
				}
				_ => {
					result.push($zero);
					bitvec.push(false);
				}
			}
		}
		ColumnBuffer::$factory(result, bitvec)
	}};
}

macro_rules! sub_sat_float {
	($a:ident, $b:ident, $row_count:expr, $factory:ident, $zero:expr) => {{
		let mut result = Vec::with_capacity($row_count);
		let mut bitvec = Vec::with_capacity($row_count);
		for i in 0..$row_count {
			match ($a.get(i), $b.get(i)) {
				(Some(&l), Some(&r)) if !l.is_nan() && !r.is_nan() => {
					let diff = l - r;
					result.push(if diff < $zero {
						$zero
					} else {
						diff
					});
					bitvec.push(true);
				}
				_ => {
					result.push($zero);
					bitvec.push(false);
				}
			}
		}
		ColumnBuffer::$factory(result, bitvec)
	}};
}

impl<'a> Routine<FunctionContext<'a>> for SubSaturate {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[Type]) -> Type {
		input_types.first().cloned().unwrap_or(Type::Float8)
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let a_col = &args[0];
		let b_col = &args[1];
		let (a_data, a_bv) = a_col.unwrap_option();
		let (b_data, b_bv) = b_col.unwrap_option();
		let row_count = a_data.len();

		if !a_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: a_data.get_type(),
			});
		}
		if !b_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: b_data.get_type(),
			});
		}

		let result_data = match (a_data, b_data) {
			(ColumnBuffer::Uint1(a), ColumnBuffer::Uint1(b)) => {
				sub_sat_int!(a, b, row_count, uint1_with_bitvec)
			}
			(ColumnBuffer::Uint2(a), ColumnBuffer::Uint2(b)) => {
				sub_sat_int!(a, b, row_count, uint2_with_bitvec)
			}
			(ColumnBuffer::Uint4(a), ColumnBuffer::Uint4(b)) => {
				sub_sat_int!(a, b, row_count, uint4_with_bitvec)
			}
			(ColumnBuffer::Uint8(a), ColumnBuffer::Uint8(b)) => {
				sub_sat_int!(a, b, row_count, uint8_with_bitvec)
			}
			(ColumnBuffer::Uint16(a), ColumnBuffer::Uint16(b)) => {
				sub_sat_int!(a, b, row_count, uint16_with_bitvec)
			}
			(ColumnBuffer::Int1(a), ColumnBuffer::Int1(b)) => {
				sub_sat_signed!(a, b, row_count, int1_with_bitvec, 0i8)
			}
			(ColumnBuffer::Int2(a), ColumnBuffer::Int2(b)) => {
				sub_sat_signed!(a, b, row_count, int2_with_bitvec, 0i16)
			}
			(ColumnBuffer::Int4(a), ColumnBuffer::Int4(b)) => {
				sub_sat_signed!(a, b, row_count, int4_with_bitvec, 0i32)
			}
			(ColumnBuffer::Int8(a), ColumnBuffer::Int8(b)) => {
				sub_sat_signed!(a, b, row_count, int8_with_bitvec, 0i64)
			}
			(ColumnBuffer::Int16(a), ColumnBuffer::Int16(b)) => {
				sub_sat_signed!(a, b, row_count, int16_with_bitvec, 0i128)
			}
			(ColumnBuffer::Float4(a), ColumnBuffer::Float4(b)) => {
				sub_sat_float!(a, b, row_count, float4_with_bitvec, 0.0f32)
			}
			(ColumnBuffer::Float8(a), ColumnBuffer::Float8(b)) => {
				sub_sat_float!(a, b, row_count, float8_with_bitvec, 0.0f64)
			}
			_ => {
				return Err(RoutineError::FunctionInvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![a_data.get_type()],
					actual: b_data.get_type(),
				});
			}
		};

		let combined_bv = match (a_bv, b_bv) {
			(Some(a), Some(b)) => Some(a.and(b)),
			(Some(a), None) => Some(a.clone()),
			(None, Some(b)) => Some(b.clone()),
			(None, None) => None,
		};

		let final_data = if let Some(bv) = combined_bv {
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

impl Function for SubSaturate {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
