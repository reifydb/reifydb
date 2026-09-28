// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::convert::identity;

use arrow_buffer::{BooleanBuffer, NullBuffer, i256};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{context::FunctionContext, error::RoutineError};
use reifydb_value::{
	error::TypeError,
	value::{
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
	support::coerce::{CoerceMode, all_rows_none, coerce_column, promote_pair},
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
	data: &ColumnBuffer,
	argument_index: usize,
) -> Result<(), RoutineError> {
	if !data.get_type().is_number() && data.get_type() != ValueType::Any {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual: data.get_type(),
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

fn make_strict_error(ctx: &FunctionContext, msg_col: &ColumnBuffer, i: usize) -> RoutineError {
	let reason = match msg_col {
		ColumnBuffer::Utf8 {
			container,
			..
		} => varlen_array::get(container, i).unwrap_or("overflow").to_string(),
		_ => "overflow".to_string(),
	};
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub fn dispatch_two<Op: ArithOp>(
	ctx: &mut FunctionContext,
	args: &Columns,
	strategy: BasicStrategy,
) -> Result<Columns, RoutineError> {
	execute_arith::<Op>(ctx, &args[0], &args[1], strategy.row_mode(), strategy.coerce_mode(), None, None)
}

pub fn dispatch_fallback<Op: ArithOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	let (d_data, _) = args[2].clone().split_nulls();
	ensure_numeric(ctx, &d_data, 2)?;
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Fallback, CoerceMode::Error, Some(&args[2]), None)
}

pub fn dispatch_strict<Op: ArithOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	let (msg_data, _) = args[2].clone().split_nulls();
	if msg_data.get_type() != ValueType::Utf8 {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 2,
			expected: vec![ValueType::Utf8],
			actual: msg_data.get_type(),
		});
	}
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Strict, CoerceMode::Error, None, Some(&msg_data))
}

fn execute_arith<Op: ArithOp>(
	ctx: &mut FunctionContext,
	a_col: &ColumnBuffer,
	b_col: &ColumnBuffer,
	mode: RowMode,
	coerce_mode: CoerceMode,
	fallback_col: Option<&ColumnBuffer>,
	strict_msg: Option<&ColumnBuffer>,
) -> Result<Columns, RoutineError> {
	let (a_data, _) = a_col.clone().split_nulls();
	let (b_data, _) = b_col.clone().split_nulls();
	ensure_numeric(ctx, &a_data, 0)?;
	ensure_numeric(ctx, &b_data, 1)?;

	let promoted = promote_pair(a_data.get_type(), b_data.get_type());
	if promoted == ValueType::Any {
		if all_rows_none(a_col) && all_rows_none(b_col) {
			let result = ColumnBuffer::none_typed(ValueType::Any, a_data.len());
			return Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]));
		}
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 0,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual: ValueType::Any,
		});
	}
	let a_operand = family_operand(&a_data.get_type(), &promoted);
	let b_operand = family_operand(&b_data.get_type(), &promoted);
	let d_operand = fallback_col.map(|d| family_operand(&unwrap_option(d.get_type()), &promoted));
	let target = family_target::<Op>(&promoted, &a_operand, &b_operand, d_operand.as_ref());

	let a_cast = coerce_column(ctx, a_col, a_operand.coerce_to.clone(), coerce_mode)?;
	let b_cast = coerce_column(ctx, b_col, b_operand.coerce_to.clone(), coerce_mode)?;
	let d_cast = match (fallback_col, &d_operand) {
		(Some(d), Some(operand)) => Some(coerce_column(ctx, d, operand.coerce_to.clone(), CoerceMode::Error)?),
		_ => None,
	};

	let (a_inner, a_bv) = (&a_cast, a_cast.nulls().map(NullBuffer::inner));
	let (b_inner, b_bv) = (&b_cast, b_cast.nulls().map(NullBuffer::inner));
	let d_parts = d_cast.as_ref().map(|d| (d, d.nulls().map(NullBuffer::inner)));

	macro_rules! run {
		($container_variant:ident) => {{
			let (ColumnBuffer::$container_variant(l), ColumnBuffer::$container_variant(r)) =
				(a_inner, b_inner)
			else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ColumnBuffer::$container_variant(c) = inner else {
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
			let (ColumnBuffer::$container_variant(l), ColumnBuffer::$container_variant(r)) =
				(a_inner, b_inner)
			else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ColumnBuffer::$container_variant(c) = inner else {
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
			let (ColumnBuffer::Int16(l), ColumnBuffer::Int16(r)) = (a_inner, b_inner) else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ColumnBuffer::Int16(c) = inner else {
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
			let (ColumnBuffer::Uint16(l), ColumnBuffer::Uint16(r)) = (a_inner, b_inner) else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ColumnBuffer::Uint16(c) = inner else {
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
			ColumnBuffer::decimal_with_bitvec(precision, scale, values, bits)
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

	Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
}

#[allow(clippy::too_many_arguments)]
fn compute_rows<T: SafeNum, Op: ArithOp>(
	ctx: &FunctionContext,
	target: &ValueType,
	l: (&[T], Option<&BooleanBuffer>),
	r: (&[T], Option<&BooleanBuffer>),
	mode: &RowMode,
	fallback: Option<(&[T], Option<&BooleanBuffer>)>,
	strict_msg: Option<&ColumnBuffer>,
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
