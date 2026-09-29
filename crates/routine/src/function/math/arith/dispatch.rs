// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::convert::identity;

use arrow_array::ArrayRef;
use arrow_buffer::{BooleanBuffer, NullBuffer, i256};
use arrow_schema::FieldRef;
use reifydb_core::value::column::{
	factory::{
		decimal_with_bitvec, float4_with_bitvec, float8_with_bitvec, int1_with_bitvec, int2_with_bitvec,
		int4_with_bitvec, int8_with_bitvec, int16_with_bitvec, none, uint1_with_bitvec, uint2_with_bitvec,
		uint4_with_bitvec, uint8_with_bitvec, uint16_with_bitvec,
	},
	nulls::split_nulls,
};
use reifydb_routine_abi::{context::FunctionContext, error::RoutineError};
use reifydb_value::{
	error::TypeError,
	value::{
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::{decimal_array::decimals, varlen_array, wide_int_array::wides},
		decimal::{Decimal, unscaled},
		is::IsNumber,
		number::safe::div::SafeDiv,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::function::{
	math::arith::op::{ArithOp, FamilyDigits, SafeNum},
	support::coerce::{CoerceMode, all_rows_none, bare_type, coerce_column, promote_pair},
};

#[derive(Debug, Clone, Copy)]
pub enum BasicStrategy {
	Default,
	Saturate,
	Wrap,
	Zero,
	None,
}

enum RowMode {
	Default,
	Saturate,
	Wrap,
	Zero,
	None,
	Strict,
	Fallback,
}

impl BasicStrategy {
	fn row_mode(&self) -> RowMode {
		match self {
			BasicStrategy::Default => RowMode::Default,
			BasicStrategy::Saturate => RowMode::Saturate,
			BasicStrategy::Wrap => RowMode::Wrap,
			BasicStrategy::Zero => RowMode::Zero,
			BasicStrategy::None => RowMode::None,
		}
	}

	fn coerce_mode(&self) -> CoerceMode {
		match self {
			BasicStrategy::None => CoerceMode::None,
			_ => CoerceMode::Error,
		}
	}
}

pub(crate) fn ensure_numeric(
	ctx: &mut FunctionContext,
	data: &ColumnView,
	argument_index: usize,
) -> Result<(), RoutineError> {
	let actual = bare_type(data);
	if !actual.is_number() && actual != ValueType::Any {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual,
		});
	}
	Ok(())
}

struct FamilyOperand {
	coerce_to: ValueType,
	digits: FamilyDigits,
}

fn family_operand(original: &ValueType, promoted: &ValueType) -> FamilyOperand {
	let (integer, scale) = match original {
		ValueType::Decimal {
			precision,
			scale,
		} => (precision.value().saturating_sub(scale.value()), Some(scale.value())),
		ValueType::Int1 | ValueType::Uint1 => (3, Some(0)),
		ValueType::Int2 | ValueType::Uint2 => (5, Some(0)),
		ValueType::Int4 | ValueType::Uint4 => (10, Some(0)),
		ValueType::Int8 => (19, Some(0)),
		ValueType::Uint8 => (20, Some(0)),
		ValueType::Int16 | ValueType::Uint16 => (39, Some(0)),
		_ => (unscaled::MAX_DIGITS, None),
	};
	match promoted {
		ValueType::Decimal {
			scale: promoted_scale,
			..
		} => {
			let scale = scale.unwrap_or(promoted_scale.value());
			FamilyOperand {
				coerce_to: ValueType::decimal(Precision::MAX, Scale::new(scale)),
				digits: FamilyDigits {
					integer,
					scale,
				},
			}
		}
		other => FamilyOperand {
			coerce_to: other.clone(),
			digits: FamilyDigits {
				integer,
				scale: 0,
			},
		},
	}
}

fn unwrap_option(ty: ValueType) -> ValueType {
	match ty {
		ValueType::Option(inner) => *inner,
		other => other,
	}
}

fn family_target<Op: ArithOp>(
	promoted: &ValueType,
	left: &FamilyOperand,
	right: &FamilyOperand,
	fallback: Option<&FamilyOperand>,
) -> ValueType {
	let mut digits = Op::family_digits(left.digits, right.digits);
	if let Some(fallback) = fallback {
		digits.integer = digits.integer.max(fallback.digits.integer);
		digits.scale = digits.scale.max(fallback.digits.scale);
	}
	let max = unscaled::MAX_DIGITS;
	let scale = digits.scale.min(max);
	match promoted {
		ValueType::Decimal {
			..
		} => ValueType::decimal(
			Precision::new(digits.integer.saturating_add(scale).clamp(1, max)),
			Scale::new(scale),
		),
		other => other.clone(),
	}
}

fn family_bound(precision: Precision, negative: bool) -> i256 {
	let max = unscaled::pow10(precision.value()).expect("a precision is at most 76 digits").wrapping_sub(i256::ONE);
	if negative {
		max.wrapping_neg()
	} else {
		max
	}
}

fn make_strict_error(ctx: &FunctionContext, msg_col: &ColumnView, i: usize) -> RoutineError {
	let reason = match &msg_col.data {
		ViewData::Utf8 {
			container,
			..
		} => varlen_array::get(*container, i).unwrap_or("overflow").to_string(),
		_ => "overflow".to_string(),
	};
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub fn dispatch_two<Op: ArithOp>(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
	strategy: BasicStrategy,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	execute_arith::<Op>(ctx, &args[0], &args[1], strategy.row_mode(), strategy.coerce_mode(), None, None)
}

pub fn dispatch_fallback<Op: ArithOp>(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let (d_bare, _) = split_nulls(args[2].clone())?;
	ensure_numeric(ctx, &ColumnView::try_from(&d_bare)?, 2)?;
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Fallback, CoerceMode::Error, Some(&args[2]), None)
}

pub fn dispatch_strict<Op: ArithOp>(
	ctx: &mut FunctionContext,
	args: &[(FieldRef, ArrayRef)],
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let (msg_bare, _) = split_nulls(args[2].clone())?;
	let msg_data = ColumnView::try_from(&msg_bare)?;
	let actual = bare_type(&msg_data);
	if actual != ValueType::Utf8 {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 2,
			expected: vec![ValueType::Utf8],
			actual,
		});
	}
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Strict, CoerceMode::Error, None, Some(&msg_data))
}

fn execute_arith<Op: ArithOp>(
	ctx: &mut FunctionContext,
	a_col: &(FieldRef, ArrayRef),
	b_col: &(FieldRef, ArrayRef),
	mode: RowMode,
	coerce_mode: CoerceMode,
	fallback_col: Option<&(FieldRef, ArrayRef)>,
	strict_msg: Option<&ColumnView>,
) -> Result<(FieldRef, ArrayRef), RoutineError> {
	let (a_bare, _) = split_nulls(a_col.clone())?;
	let (b_bare, _) = split_nulls(b_col.clone())?;
	let a_data = ColumnView::try_from(&a_bare)?;
	let b_data = ColumnView::try_from(&b_bare)?;
	ensure_numeric(ctx, &a_data, 0)?;
	ensure_numeric(ctx, &b_data, 1)?;

	let a_view = ColumnView::try_from(a_col)?;
	let b_view = ColumnView::try_from(b_col)?;
	let d_view = fallback_col.map(ColumnView::try_from).transpose()?;

	let promoted = promote_pair(bare_type(&a_data), bare_type(&b_data));
	if promoted == ValueType::Any {
		if all_rows_none(&a_view) && all_rows_none(&b_view) {
			return Ok(none(ctx.fragment.text(), a_data.len()));
		}
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 0,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual: ValueType::Any,
		});
	}
	let a_operand = family_operand(&bare_type(&a_data), &promoted);
	let b_operand = family_operand(&bare_type(&b_data), &promoted);
	let d_operand = d_view.as_ref().map(|d| family_operand(&unwrap_option(d.get_type()), &promoted));
	let target = family_target::<Op>(&promoted, &a_operand, &b_operand, d_operand.as_ref());

	let a_cast = coerce_column(ctx, &a_view, a_operand.coerce_to.clone(), coerce_mode)?;
	let b_cast = coerce_column(ctx, &b_view, b_operand.coerce_to.clone(), coerce_mode)?;
	let d_cast = match (&d_view, &d_operand) {
		(Some(d), Some(operand)) => Some(coerce_column(ctx, d, operand.coerce_to.clone(), CoerceMode::Error)?),
		_ => None,
	};

	let a_inner = ColumnView::try_from(&a_cast)?;
	let b_inner = ColumnView::try_from(&b_cast)?;
	let d_inner = d_cast.as_ref().map(ColumnView::try_from).transpose()?;
	let a_nulls = a_inner.logical_nulls();
	let b_nulls = b_inner.logical_nulls();
	let d_nulls = d_inner.as_ref().and_then(ColumnView::logical_nulls);
	let a_bv = a_nulls.as_ref().map(NullBuffer::inner);
	let b_bv = b_nulls.as_ref().map(NullBuffer::inner);
	let d_parts = d_inner.as_ref().map(|d| (d, d_nulls.as_ref().map(NullBuffer::inner)));

	macro_rules! run {
		($container_variant:ident) => {{
			let (ViewData::$container_variant(l), ViewData::$container_variant(r)) =
				(&a_inner.data, &b_inner.data)
			else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ViewData::$container_variant(c) = &inner.data else {
					unreachable!()
				};
				(&c.values()[..], *bv)
			});
			compute_rows::<_, Op>(
				ctx,
				&target,
				(l.values(), a_bv),
				(r.values(), b_bv),
				&mode,
				d,
				strict_msg,
				Some,
				identity,
			)?
		}};
		($container_variant:ident(..), $decode:ident, $fit:expr, $clamp:expr) => {{
			let (ViewData::$container_variant(l), ViewData::$container_variant(r)) =
				(&a_inner.data, &b_inner.data)
			else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ViewData::$container_variant(c) = &inner.data else {
					unreachable!()
				};
				($decode(c), *bv)
			});
			compute_rows::<_, Op>(
				ctx,
				&target,
				(&$decode(l), a_bv),
				(&$decode(r), b_bv),
				&mode,
				d.as_ref().map(|(values, bv)| (&values[..], *bv)),
				strict_msg,
				$fit,
				$clamp,
			)?
		}};
	}

	let result = match target {
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
			let (ViewData::Int16(l), ViewData::Int16(r)) = (&a_inner.data, &b_inner.data) else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ViewData::Int16(c) = &inner.data else {
					unreachable!()
				};
				(wides::<i128>(c), *bv)
			});
			let (values, bits) = compute_rows::<_, Op>(
				ctx,
				&target,
				(&wides::<i128>(l), a_bv),
				(&wides::<i128>(r), b_bv),
				&mode,
				d.as_ref().map(|(values, bv)| (&values[..], *bv)),
				strict_msg,
				Some,
				identity,
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
			let (ViewData::Uint16(l), ViewData::Uint16(r)) = (&a_inner.data, &b_inner.data) else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ViewData::Uint16(c) = &inner.data else {
					unreachable!()
				};
				(wides::<u128>(c), *bv)
			});
			let (values, bits) = compute_rows::<_, Op>(
				ctx,
				&target,
				(&wides::<u128>(l), a_bv),
				(&wides::<u128>(r), b_bv),
				&mode,
				d.as_ref().map(|(values, bv)| (&values[..], *bv)),
				strict_msg,
				Some,
				identity,
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
			let (values, bits) = run!(
				Decimal(..),
				decimals,
				|value: Decimal| value.fits(precision.value(), scale.value()),
				|value: Decimal| {
					value.round_to_scale(scale.value())
						.and_then(|value| value.fits(precision.value(), scale.value()))
						.unwrap_or_else(|| {
							Decimal::from_parts(
								family_bound(precision, value.is_negative()),
								scale.value(),
							)
							.expect("a precision bound is in range")
						})
				}
			);
			decimal_with_bitvec(ctx.fragment.text(), precision, scale, values, bits)
		}
		other => {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other,
			});
		}
	};

	Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn compute_rows<T: SafeNum, Op: ArithOp>(
	ctx: &FunctionContext,
	target: &ValueType,
	l: (&[T], Option<&BooleanBuffer>),
	r: (&[T], Option<&BooleanBuffer>),
	mode: &RowMode,
	fallback: Option<(&[T], Option<&BooleanBuffer>)>,
	strict_msg: Option<&ColumnView>,
	fit: impl Fn(T) -> Option<T>,
	clamp: impl Fn(T) -> T,
) -> Result<(Vec<T>, Vec<bool>), RoutineError> {
	fn defined<T: IsNumber>(c: &[T], bv: Option<&BooleanBuffer>, i: usize) -> bool {
		i < c.len() && bv.is_none_or(|b| b.value(i))
	}

	let (l, l_bv) = l;
	let (r, r_bv) = r;
	let row_count = l.len();
	let mut values = Vec::with_capacity(row_count);
	let mut bits = Vec::with_capacity(row_count);

	for i in 0..row_count {
		if !defined(l, l_bv, i) || !defined(r, r_bv, i) {
			values.push(T::default());
			bits.push(false);
			continue;
		}
		let lv = l.get(i).expect("defined row has a value");
		let rv = r.get(i).expect("defined row has a value");

		let out_of_range = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: target.clone(),
				fragment: ctx.fragment.clone(),
				descriptor: None,
			}
			.into()
		};
		let value = match mode {
			RowMode::Default => {
				if Op::DIVISIVE && SafeDiv::is_zero(rv) {
					return Err(TypeError::DivisionByZero {
						target: target.clone(),
						fragment: ctx.fragment.clone(),
					}
					.into());
				}
				Op::checked(lv, rv).and_then(&fit).ok_or_else(out_of_range)?
			}
			RowMode::Strict => match Op::checked(lv, rv).and_then(&fit) {
				Some(v) => v,
				None => {
					return Err(make_strict_error(
						ctx,
						strict_msg.expect("strict mode carries a message column"),
						i,
					));
				}
			},
			RowMode::Saturate => clamp(Op::saturating(lv, rv)),
			RowMode::Wrap => clamp(Op::wrapping(lv, rv)),
			RowMode::Zero => Op::checked(lv, rv).and_then(&fit).unwrap_or_default(),
			RowMode::None => match Op::checked(lv, rv).and_then(&fit) {
				Some(v) => v,
				None => {
					values.push(T::default());
					bits.push(false);
					continue;
				}
			},
			RowMode::Fallback => match Op::checked(lv, rv).and_then(&fit) {
				Some(v) => v,
				None => {
					let (d, d_bv) = fallback.expect("fallback mode carries a fallback column");
					if defined(d, d_bv, i) {
						fit(d.get(i).expect("defined row has a value").clone())
							.ok_or_else(out_of_range)?
					} else {
						values.push(T::default());
						bits.push(false);
						continue;
					}
				}
			},
		};
		values.push(value);
		bits.push(true);
	}

	Ok((values, bits))
}
