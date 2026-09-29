// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Display;

use arrow_array::ArrayRef;
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{
		decimal_with_bitvec, float4_with_bitvec, float8_with_bitvec, int1_with_bitvec, int2_with_bitvec,
		int4_with_bitvec, int8_with_bitvec, int16_with_bitvec, none, uint1_with_bitvec, uint2_with_bitvec,
		uint4_with_bitvec, uint8_with_bitvec, uint16_with_bitvec,
	},
	nulls::split_nulls,
};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	container::{decimal_array::decimals, wide_int_array::wides},
	is::IsNumber,
	value_type::ValueType,
};

use crate::function::{
	math::arith::dispatch::ensure_numeric,
	support::coerce::{CoerceMode, all_rows_none, bare_type, coerce_column, promote_all},
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

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let mut types = Vec::with_capacity(3);
		for (i, arg) in args.iter().enumerate().take(3) {
			let (bare, _) = split_nulls(arg.clone())?;
			let data = ColumnView::try_from(&bare)?;
			ensure_numeric(ctx, &data, i)?;
			types.push(bare_type(&data));
		}
		let views = args.iter().take(3).map(ColumnView::try_from).collect::<Result<Vec<_>, _>>()?;

		let promoted = promote_all(types);
		if promoted == ValueType::Any {
			if (0..3).all(|i| all_rows_none(&views[i])) {
				let row_count = views[0].len();
				return Ok(none(ctx.fragment.text(), row_count));
			}
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![],
				actual: ValueType::Any,
			});
		}
		let v_cast = coerce_column(ctx, &views[0], promoted.clone(), CoerceMode::Error)?;
		let lo_cast = coerce_column(ctx, &views[1], promoted.clone(), CoerceMode::Error)?;
		let hi_cast = coerce_column(ctx, &views[2], promoted.clone(), CoerceMode::Error)?;

		let v_inner = ColumnView::try_from(&v_cast)?;
		let lo_inner = ColumnView::try_from(&lo_cast)?;
		let hi_inner = ColumnView::try_from(&hi_cast)?;
		let v_nulls = v_inner.logical_nulls();
		let lo_nulls = lo_inner.logical_nulls();
		let hi_nulls = hi_inner.logical_nulls();
		let v_bv = v_nulls.as_ref().map(NullBuffer::inner);
		let lo_bv = lo_nulls.as_ref().map(NullBuffer::inner);
		let hi_bv = hi_nulls.as_ref().map(NullBuffer::inner);

		macro_rules! run {
			($variant:ident) => {{
				let (ViewData::$variant(v), ViewData::$variant(lo), ViewData::$variant(hi)) =
					(&v_inner.data, &lo_inner.data, &hi_inner.data)
				else {
					unreachable!()
				};
				clamp_rows(ctx, v.values(), v_bv, lo.values(), lo_bv, hi.values(), hi_bv)?
			}};
			($variant:ident(..), $decode:ident) => {{
				let (ViewData::$variant(v), ViewData::$variant(lo), ViewData::$variant(hi)) =
					(&v_inner.data, &lo_inner.data, &hi_inner.data)
				else {
					unreachable!()
				};
				clamp_rows(ctx, &$decode(v), v_bv, &$decode(lo), lo_bv, &$decode(hi), hi_bv)?
			}};
		}

		let result = match promoted {
			ValueType::Int1 => {
				let (values, bits) = run!(Int1);
				int1_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Int2 => {
				let (values, bits) = run!(Int2);
				int2_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Int4 => {
				let (values, bits) = run!(Int4);
				int4_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Int8 => {
				let (values, bits) = run!(Int8);
				int8_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Int16 => {
				let (ViewData::Int16(v), ViewData::Int16(lo), ViewData::Int16(hi)) =
					(&v_inner.data, &lo_inner.data, &hi_inner.data)
				else {
					unreachable!()
				};
				let (values, bits) = clamp_rows(
					ctx,
					&wides::<i128>(v),
					v_bv,
					&wides::<i128>(lo),
					lo_bv,
					&wides::<i128>(hi),
					hi_bv,
				)?;
				int16_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Uint1 => {
				let (values, bits) = run!(Uint1);
				uint1_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Uint2 => {
				let (values, bits) = run!(Uint2);
				uint2_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Uint4 => {
				let (values, bits) = run!(Uint4);
				uint4_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Uint8 => {
				let (values, bits) = run!(Uint8);
				uint8_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Uint16 => {
				let (ViewData::Uint16(v), ViewData::Uint16(lo), ViewData::Uint16(hi)) =
					(&v_inner.data, &lo_inner.data, &hi_inner.data)
				else {
					unreachable!()
				};
				let (values, bits) = clamp_rows(
					ctx,
					&wides::<u128>(v),
					v_bv,
					&wides::<u128>(lo),
					lo_bv,
					&wides::<u128>(hi),
					hi_bv,
				)?;
				uint16_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Float4 => {
				let (values, bits) = run!(Float4);
				float4_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Float8 => {
				let (values, bits) = run!(Float8);
				float8_with_bitvec(ctx.fragment.text(), values, bits)
			}
			ValueType::Decimal {
				precision,
				scale,
			} => {
				let (values, bits) = run!(Decimal(..), decimals);
				decimal_with_bitvec(ctx.fragment.text(), precision, scale, values, bits)
			}
			_ => unreachable!("promotion of numeric inputs yields a numeric type"),
		};

		Ok(result)
	}
}

fn clamp_rows<T>(
	ctx: &FunctionContext,
	v: &[T],
	v_bv: Option<&BooleanBuffer>,
	lo: &[T],
	lo_bv: Option<&BooleanBuffer>,
	hi: &[T],
	hi_bv: Option<&BooleanBuffer>,
) -> Result<(Vec<T>, Vec<bool>), RoutineError>
where
	T: IsNumber + PartialOrd + Clone + Default + Display,
{
	fn defined<T: IsNumber>(c: &[T], bv: Option<&BooleanBuffer>, i: usize) -> bool {
		i < c.len() && bv.is_none_or(|b| b.value(i))
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

	fn arity(&self) -> Arity {
		Arity::Exact(3)
	}
}
