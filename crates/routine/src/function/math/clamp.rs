// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	util::bitvec::BitVec,
	value::{container::number::NumberContainer, is::IsNumber, value_type::ValueType},
};

use crate::{
	function::{
		math::arith::dispatch::{ensure_arity, ensure_numeric},
		support::coerce::{CoercePolicy, all_rows_none, coerce_column, promote_all},
	},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct Clamp {
	info: RoutineInfo,
}

impl Default for Clamp {
	fn default() -> Self {
		Self::new()
	}
}

impl Clamp {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::clamp"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Clamp {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		if input_types.len() >= 3
			&& input_types[0].is_number()
			&& input_types[1].is_number()
			&& input_types[2].is_number()
		{
			promote_all(input_types.iter().take(3).cloned())
		} else {
			ValueType::Float8
		}
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		ensure_arity(ctx, args, 3)?;

		for i in 0..3 {
			let (data, _) = args[i].unwrap_option();
			ensure_numeric(ctx, data, i)?;
		}

		let promoted = promote_all((0..3).map(|i| args[i].unwrap_option().0.get_type()));
		if promoted == ValueType::Any {
			if (0..3).all(|i| all_rows_none(&args[i])) {
				let row_count = args[0].unwrap_option().0.len();
				let result = ColumnBuffer::none_typed(ValueType::Any, row_count);
				return Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]));
			}
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![],
				actual: ValueType::Any,
			});
		}
		let v_cast = coerce_column(ctx, &args[0], promoted.clone(), CoercePolicy::Error)?;
		let lo_cast = coerce_column(ctx, &args[1], promoted.clone(), CoercePolicy::Error)?;
		let hi_cast = coerce_column(ctx, &args[2], promoted.clone(), CoercePolicy::Error)?;

		let (v_inner, v_bv) = v_cast.unwrap_option();
		let (lo_inner, lo_bv) = lo_cast.unwrap_option();
		let (hi_inner, hi_bv) = hi_cast.unwrap_option();

		macro_rules! run {
			($variant:ident) => {{
				let (ColumnBuffer::$variant(v), ColumnBuffer::$variant(lo), ColumnBuffer::$variant(hi)) =
					(v_inner, lo_inner, hi_inner)
				else {
					unreachable!()
				};
				clamp_rows(ctx, v, v_bv, lo, lo_bv, hi, hi_bv)?
			}};
			($variant:ident { .. }) => {{
				let (
					ColumnBuffer::$variant {
						container: v,
						..
					},
					ColumnBuffer::$variant {
						container: lo,
						..
					},
					ColumnBuffer::$variant {
						container: hi,
						..
					},
				) = (v_inner, lo_inner, hi_inner)
				else {
					unreachable!()
				};
				clamp_rows(ctx, v, v_bv, lo, lo_bv, hi, hi_bv)?
			}};
		}

		let result = match promoted {
			ValueType::Int1 => {
				let (values, bits) = run!(Int1);
				ColumnBuffer::int1_with_bitvec(values, bits)
			}
			ValueType::Int2 => {
				let (values, bits) = run!(Int2);
				ColumnBuffer::int2_with_bitvec(values, bits)
			}
			ValueType::Int4 => {
				let (values, bits) = run!(Int4);
				ColumnBuffer::int4_with_bitvec(values, bits)
			}
			ValueType::Int8 => {
				let (values, bits) = run!(Int8);
				ColumnBuffer::int8_with_bitvec(values, bits)
			}
			ValueType::Int16 => {
				let (values, bits) = run!(Int16);
				ColumnBuffer::int16_with_bitvec(values, bits)
			}
			ValueType::Uint1 => {
				let (values, bits) = run!(Uint1);
				ColumnBuffer::uint1_with_bitvec(values, bits)
			}
			ValueType::Uint2 => {
				let (values, bits) = run!(Uint2);
				ColumnBuffer::uint2_with_bitvec(values, bits)
			}
			ValueType::Uint4 => {
				let (values, bits) = run!(Uint4);
				ColumnBuffer::uint4_with_bitvec(values, bits)
			}
			ValueType::Uint8 => {
				let (values, bits) = run!(Uint8);
				ColumnBuffer::uint8_with_bitvec(values, bits)
			}
			ValueType::Uint16 => {
				let (values, bits) = run!(Uint16);
				ColumnBuffer::uint16_with_bitvec(values, bits)
			}
			ValueType::Float4 => {
				let (values, bits) = run!(Float4);
				ColumnBuffer::float4_with_bitvec(values, bits)
			}
			ValueType::Float8 => {
				let (values, bits) = run!(Float8);
				ColumnBuffer::float8_with_bitvec(values, bits)
			}
			ValueType::Int => {
				let (values, bits) = run!(Int { .. });
				ColumnBuffer::int_with_bitvec(values, bits)
			}
			ValueType::Uint => {
				let (values, bits) = run!(Uint { .. });
				ColumnBuffer::uint_with_bitvec(values, bits)
			}
			ValueType::Decimal => {
				let (values, bits) = run!(Decimal { .. });
				ColumnBuffer::decimal_with_bitvec(values, bits)
			}
			_ => unreachable!("promotion of numeric inputs yields a numeric type"),
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
	}
}

fn clamp_rows<T>(
	ctx: &FunctionContext,
	v: &NumberContainer<T>,
	v_bv: Option<&BitVec>,
	lo: &NumberContainer<T>,
	lo_bv: Option<&BitVec>,
	hi: &NumberContainer<T>,
	hi_bv: Option<&BitVec>,
) -> Result<(Vec<T>, Vec<bool>), RoutineError>
where
	T: IsNumber + PartialOrd + Clone + Default + Display,
{
	fn defined<T: IsNumber>(c: &NumberContainer<T>, bv: Option<&BitVec>, i: usize) -> bool {
		c.is_defined(i) && bv.is_none_or(|b| b.get(i))
	}

	let row_count = v.len();
	let mut values = Vec::with_capacity(row_count);
	let mut bits = Vec::with_capacity(row_count);

	for i in 0..row_count {
		if !defined(v, v_bv, i) || !defined(lo, lo_bv, i) || !defined(hi, hi_bv, i) {
			values.push(T::default());
			bits.push(false);
			continue;
		}
		let val = v.get(i).expect("defined row has a value");
		let min = lo.get(i).expect("defined row has a value");
		let max = hi.get(i).expect("defined row has a value");

		if min > max {
			return Err(RoutineError::FunctionExecutionFailed {
				function: ctx.fragment.clone(),
				reason: format!("clamp lower bound {} exceeds upper bound {}", min, max),
			});
		}

		let clamped = if val < min {
			min.clone()
		} else if val > max {
			max.clone()
		} else {
			val.clone()
		};
		values.push(clamped);
		bits.push(true);
	}

	Ok((values, bits))
}

impl Function for Clamp {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
