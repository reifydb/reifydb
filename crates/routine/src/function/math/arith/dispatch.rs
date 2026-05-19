// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{
		decimal::Decimal,
		int::Int,
		r#type::{Type, input_types::InputTypes},
		uint::Uint,
	},
};

use crate::{
	function::math::arith::{
		cast::{
			convert_column_to_type, get_as_big_int, get_as_big_uint, get_as_decimal, get_as_f32,
			get_as_f64, get_as_i8, get_as_i16, get_as_i32, get_as_i64, get_as_i128, get_as_u8, get_as_u16,
			get_as_u32, get_as_u64, get_as_u128, promote_two,
		},
		op::BinaryOp,
	},
	routine::{context::FunctionContext, error::RoutineError},
};

#[derive(Debug, Clone, Copy)]
pub enum BasicStrategy {
	Saturate,
	Zero,
	Null,
	Wrap,
}

enum Strategy<'a> {
	Saturate,
	Zero,
	Null,
	Default(&'a ColumnBuffer),
	Strict(&'a ColumnBuffer),
	Wrap,
}

impl BasicStrategy {
	fn as_strategy(&self) -> Strategy<'static> {
		match self {
			BasicStrategy::Saturate => Strategy::Saturate,
			BasicStrategy::Zero => Strategy::Zero,
			BasicStrategy::Null => Strategy::Null,
			BasicStrategy::Wrap => Strategy::Wrap,
		}
	}
}

fn ensure_arity(ctx: &mut FunctionContext, args: &Columns, expected: usize) -> Result<(), RoutineError> {
	if args.len() != expected {
		return Err(RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected,
			actual: args.len(),
		});
	}
	Ok(())
}

fn ensure_numeric(ctx: &mut FunctionContext, data: &ColumnBuffer, argument_index: usize) -> Result<(), RoutineError> {
	if !data.get_type().is_number() {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index,
			expected: InputTypes::numeric().expected_at(0).to_vec(),
			actual: data.get_type(),
		});
	}
	Ok(())
}

fn make_strict_error(ctx: &FunctionContext, msg_col: &ColumnBuffer, i: usize) -> RoutineError {
	let reason = match msg_col {
		ColumnBuffer::Utf8 {
			container,
			..
		} => container.get(i).unwrap_or("overflow").to_string(),
		_ => "overflow".to_string(),
	};
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

pub fn dispatch_two<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	args: &Columns,
	basic_strategy: BasicStrategy,
) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 2)?;
	execute_promoted::<Op>(ctx, &args[0], &args[1], basic_strategy.as_strategy())
}

pub fn dispatch_default<Op: BinaryOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 3)?;
	let a_col = &args[0];
	let b_col = &args[1];
	let d_col = &args[2];

	let (a_data, _) = a_col.unwrap_option();
	let (b_data, _) = b_col.unwrap_option();
	let (d_data, _) = d_col.unwrap_option();

	ensure_numeric(ctx, a_data, 0)?;
	ensure_numeric(ctx, b_data, 1)?;
	ensure_numeric(ctx, d_data, 2)?;

	let promoted = promote_two(a_data.get_type(), b_data.get_type());
	let row_count = a_data.len();
	let default_cast = convert_column_to_type(d_data, promoted.clone(), row_count);

	execute_inner::<Op>(ctx, a_col, b_col, Strategy::Default(&default_cast))
}

pub fn dispatch_strict<Op: BinaryOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 3)?;
	let a_col = &args[0];
	let b_col = &args[1];
	let msg_col = &args[2];

	let (a_data, a_bv) = a_col.unwrap_option();
	let (b_data, b_bv) = b_col.unwrap_option();
	let (msg_data, _) = msg_col.unwrap_option();

	ensure_numeric(ctx, a_data, 0)?;
	ensure_numeric(ctx, b_data, 1)?;
	if msg_data.get_type() != Type::Utf8 {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 2,
			expected: vec![Type::Utf8],
			actual: msg_data.get_type(),
		});
	}

	if a_data.get_type() != b_data.get_type() {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 1,
			expected: vec![a_data.get_type()],
			actual: b_data.get_type(),
		});
	}

	let combined_bv = match (a_bv, b_bv) {
		(Some(a), Some(b)) => Some(a.and(b)),
		(Some(a), None) => Some(a.clone()),
		(None, Some(b)) => Some(b.clone()),
		(None, None) => None,
	};
	execute_same_type::<Op>(ctx, a_col, b_col, Strategy::Strict(msg_data), combined_bv)
}

fn execute_promoted<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a_col: &ColumnBuffer,
	b_col: &ColumnBuffer,
	strategy: Strategy,
) -> Result<Columns, RoutineError> {
	let (a_data, _) = a_col.unwrap_option();
	let (b_data, _) = b_col.unwrap_option();

	ensure_numeric(ctx, a_data, 0)?;
	ensure_numeric(ctx, b_data, 1)?;

	execute_inner::<Op>(ctx, a_col, b_col, strategy)
}

fn execute_inner<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a_col: &ColumnBuffer,
	b_col: &ColumnBuffer,
	strategy: Strategy,
) -> Result<Columns, RoutineError> {
	let (a_data, _) = a_col.unwrap_option();
	let (b_data, _) = b_col.unwrap_option();

	let promoted = promote_two(a_data.get_type(), b_data.get_type());
	let row_count = a_data.len();
	let a_cast = convert_column_to_type(a_data, promoted.clone(), row_count);
	let b_cast = convert_column_to_type(b_data, promoted.clone(), row_count);
	let (a_inner, _) = a_cast.unwrap_option();
	let (b_inner, _) = b_cast.unwrap_option();

	let result = compute::<Op>(ctx, a_inner, b_inner, &strategy, promoted, row_count, None)?;
	Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
}

fn execute_same_type<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a_col: &ColumnBuffer,
	b_col: &ColumnBuffer,
	strategy: Strategy,
	input_bv: Option<BitVec>,
) -> Result<Columns, RoutineError> {
	let (a_data, _) = a_col.unwrap_option();
	let (b_data, _) = b_col.unwrap_option();
	let same_type = a_data.get_type();
	let row_count = a_data.len();

	let result = compute::<Op>(ctx, a_data, b_data, &strategy, same_type, row_count, input_bv.as_ref())?;
	Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
}

fn compute<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a: &ColumnBuffer,
	b: &ColumnBuffer,
	strategy: &Strategy,
	promoted: Type,
	row_count: usize,
	input_bv: Option<&BitVec>,
) -> Result<ColumnBuffer, RoutineError> {
	let is_null_input = |i: usize| -> bool { input_bv.is_some_and(|bv| i < bv.len() && !bv.get(i)) };
	macro_rules! per_int {
		($T:ty, $factory:ident, $checked:ident, $saturating:ident, $wrapping:ident, $extract:path, $zero:expr) => {{
			let mut result = Vec::<$T>::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if is_null_input(i) || !a.is_defined(i) || !b.is_defined(i) {
					result.push($zero);
					bitvec.push(false);
					continue;
				}
				let l: $T = $extract(a, i);
				let r: $T = $extract(b, i);
				match strategy {
					Strategy::Saturate => {
						result.push(<Op as BinaryOp>::$saturating(l, r));
						bitvec.push(true);
					}
					Strategy::Zero => match <Op as BinaryOp>::$checked(l, r) {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push($zero);
							bitvec.push(true);
						}
					},
					Strategy::Null => match <Op as BinaryOp>::$checked(l, r) {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push($zero);
							bitvec.push(false);
						}
					},
					Strategy::Default(d) => match <Op as BinaryOp>::$checked(l, r) {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => {
							result.push($extract(d, i));
							bitvec.push(true);
						}
					},
					Strategy::Strict(msg) => match <Op as BinaryOp>::$checked(l, r) {
						Some(v) => {
							result.push(v);
							bitvec.push(true);
						}
						None => return Err(make_strict_error(ctx, msg, i)),
					},
					Strategy::Wrap => {
						result.push(<Op as BinaryOp>::$wrapping(l, r));
						bitvec.push(true);
					}
				}
			}
			ColumnBuffer::$factory(result, bitvec)
		}};
	}

	macro_rules! per_float {
		($T:ty, $factory:ident, $eval:ident, $extract:path, $max:expr, $min:expr) => {{
			let mut result = Vec::<$T>::with_capacity(row_count);
			let mut bitvec = Vec::with_capacity(row_count);
			for i in 0..row_count {
				if is_null_input(i) || !a.is_defined(i) || !b.is_defined(i) {
					result.push(0.0);
					bitvec.push(false);
					continue;
				}
				let l: $T = $extract(a, i);
				let r: $T = $extract(b, i);
				let raw = <Op as BinaryOp>::$eval(l, r);

				if matches!(strategy, Strategy::Wrap) || raw.is_finite() {
					result.push(raw);
					bitvec.push(true);
					continue;
				}

				match strategy {
					Strategy::Saturate => {
						let clamped = if raw.is_nan() || raw > 0.0 {
							$max
						} else {
							$min
						};
						result.push(clamped);
						bitvec.push(true);
					}
					Strategy::Zero => {
						result.push(0.0);
						bitvec.push(true);
					}
					Strategy::Null => {
						result.push(0.0);
						bitvec.push(false);
					}
					Strategy::Default(d) => {
						result.push($extract(d, i));
						bitvec.push(true);
					}
					Strategy::Strict(msg) => return Err(make_strict_error(ctx, msg, i)),
					Strategy::Wrap => unreachable!(),
				}
			}
			ColumnBuffer::$factory(result, bitvec)
		}};
	}

	let result_data = match promoted {
		Type::Int1 => per_int!(i8, int1_with_bitvec, checked_i8, saturating_i8, wrapping_i8, get_as_i8, 0i8),
		Type::Int2 => {
			per_int!(i16, int2_with_bitvec, checked_i16, saturating_i16, wrapping_i16, get_as_i16, 0i16)
		}
		Type::Int4 => {
			per_int!(i32, int4_with_bitvec, checked_i32, saturating_i32, wrapping_i32, get_as_i32, 0i32)
		}
		Type::Int8 => {
			per_int!(i64, int8_with_bitvec, checked_i64, saturating_i64, wrapping_i64, get_as_i64, 0i64)
		}
		Type::Int16 => per_int!(
			i128,
			int16_with_bitvec,
			checked_i128,
			saturating_i128,
			wrapping_i128,
			get_as_i128,
			0i128
		),
		Type::Uint1 => per_int!(u8, uint1_with_bitvec, checked_u8, saturating_u8, wrapping_u8, get_as_u8, 0u8),
		Type::Uint2 => {
			per_int!(u16, uint2_with_bitvec, checked_u16, saturating_u16, wrapping_u16, get_as_u16, 0u16)
		}
		Type::Uint4 => {
			per_int!(u32, uint4_with_bitvec, checked_u32, saturating_u32, wrapping_u32, get_as_u32, 0u32)
		}
		Type::Uint8 => {
			per_int!(u64, uint8_with_bitvec, checked_u64, saturating_u64, wrapping_u64, get_as_u64, 0u64)
		}
		Type::Uint16 => per_int!(
			u128,
			uint16_with_bitvec,
			checked_u128,
			saturating_u128,
			wrapping_u128,
			get_as_u128,
			0u128
		),
		Type::Float4 => per_float!(f32, float4_with_bitvec, f32_eval, get_as_f32, f32::MAX, f32::MIN),
		Type::Float8 => per_float!(f64, float8_with_bitvec, f64_eval, get_as_f64, f64::MAX, f64::MIN),
		Type::Int => compute_big_int::<Op>(ctx, a, b, strategy, row_count, input_bv)?,
		Type::Uint => compute_big_uint::<Op>(ctx, a, b, strategy, row_count, input_bv)?,
		Type::Decimal => compute_decimal::<Op>(ctx, a, b, strategy, row_count, input_bv)?,
		other => {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: other,
			});
		}
	};

	Ok(result_data)
}

fn compute_big_int<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a: &ColumnBuffer,
	b: &ColumnBuffer,
	strategy: &Strategy,
	row_count: usize,
	input_bv: Option<&BitVec>,
) -> Result<ColumnBuffer, RoutineError> {
	let is_null_input = |i: usize| -> bool { input_bv.is_some_and(|bv| i < bv.len() && !bv.get(i)) };
	let mut result: Vec<Int> = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if is_null_input(i) || !a.is_defined(i) || !b.is_defined(i) {
			result.push(Int::zero());
			bitvec.push(false);
			continue;
		}
		let l = get_as_big_int(a, i);
		let r = get_as_big_int(b, i);
		match <Op as BinaryOp>::int_eval_checked(&l, &r) {
			Some(v) => {
				result.push(v);
				bitvec.push(true);
			}
			None => match strategy {
				Strategy::Saturate => {
					let zero = Int::zero();
					if l.0 >= zero.0 {
						result.push(Int::from_i128(i128::MAX));
					} else {
						result.push(Int::from_i128(i128::MIN));
					}
					bitvec.push(true);
				}
				Strategy::Zero | Strategy::Wrap => {
					result.push(Int::zero());
					bitvec.push(true);
				}
				Strategy::Null => {
					result.push(Int::zero());
					bitvec.push(false);
				}
				Strategy::Default(d) => {
					result.push(get_as_big_int(d, i));
					bitvec.push(true);
				}
				Strategy::Strict(msg) => return Err(make_strict_error(ctx, msg, i)),
			},
		}
	}
	Ok(ColumnBuffer::int_with_bitvec(result, bitvec))
}

fn compute_big_uint<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a: &ColumnBuffer,
	b: &ColumnBuffer,
	strategy: &Strategy,
	row_count: usize,
	input_bv: Option<&BitVec>,
) -> Result<ColumnBuffer, RoutineError> {
	let is_null_input = |i: usize| -> bool { input_bv.is_some_and(|bv| i < bv.len() && !bv.get(i)) };
	let mut result: Vec<Uint> = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if is_null_input(i) || !a.is_defined(i) || !b.is_defined(i) {
			result.push(Uint::zero());
			bitvec.push(false);
			continue;
		}
		let l = get_as_big_uint(a, i);
		let r = get_as_big_uint(b, i);
		match <Op as BinaryOp>::uint_eval_checked(&l, &r) {
			Some(v) => {
				result.push(v);
				bitvec.push(true);
			}
			None => match strategy {
				Strategy::Saturate => {
					result.push(Uint::from(u128::MAX));
					bitvec.push(true);
				}
				Strategy::Zero | Strategy::Wrap => {
					result.push(Uint::zero());
					bitvec.push(true);
				}
				Strategy::Null => {
					result.push(Uint::zero());
					bitvec.push(false);
				}
				Strategy::Default(d) => {
					result.push(get_as_big_uint(d, i));
					bitvec.push(true);
				}
				Strategy::Strict(msg) => return Err(make_strict_error(ctx, msg, i)),
			},
		}
	}
	Ok(ColumnBuffer::uint_with_bitvec(result, bitvec))
}

fn compute_decimal<Op: BinaryOp>(
	ctx: &mut FunctionContext,
	a: &ColumnBuffer,
	b: &ColumnBuffer,
	strategy: &Strategy,
	row_count: usize,
	input_bv: Option<&BitVec>,
) -> Result<ColumnBuffer, RoutineError> {
	let is_null_input = |i: usize| -> bool { input_bv.is_some_and(|bv| i < bv.len() && !bv.get(i)) };
	let mut result: Vec<Decimal> = Vec::with_capacity(row_count);
	let mut bitvec = Vec::with_capacity(row_count);
	for i in 0..row_count {
		if is_null_input(i) || !a.is_defined(i) || !b.is_defined(i) {
			result.push(Decimal::default());
			bitvec.push(false);
			continue;
		}
		let l = get_as_decimal(a, i);
		let r = get_as_decimal(b, i);
		match <Op as BinaryOp>::decimal_eval_checked(&l, &r) {
			Some(v) => {
				result.push(v);
				bitvec.push(true);
			}
			None => match strategy {
				Strategy::Saturate | Strategy::Zero | Strategy::Wrap => {
					result.push(Decimal::default());
					bitvec.push(true);
				}
				Strategy::Null => {
					result.push(Decimal::default());
					bitvec.push(false);
				}
				Strategy::Default(d) => {
					result.push(get_as_decimal(d, i));
					bitvec.push(true);
				}
				Strategy::Strict(msg) => return Err(make_strict_error(ctx, msg, i)),
			},
		}
	}
	Ok(ColumnBuffer::decimal_with_bitvec(result, bitvec))
}
