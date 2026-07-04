// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	error::TypeError,
	util::bitvec::BitVec,
	value::{
		container::number::NumberContainer,
		is::IsNumber,
		number::safe::div::SafeDiv,
		value_type::{ValueType, input_types::InputTypes},
	},
};

use crate::{
	function::{
		math::arith::op::{ArithOp, SafeNum},
		support::coerce::{CoercePolicy, all_rows_none, coerce_column, promote_pair},
	},
	routine::{context::FunctionContext, error::RoutineError},
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

	fn coerce_policy(&self) -> CoercePolicy {
		match self {
			BasicStrategy::None => CoercePolicy::None,
			_ => CoercePolicy::Error,
		}
	}
}

pub(crate) fn ensure_arity(ctx: &mut FunctionContext, args: &Columns, expected: usize) -> Result<(), RoutineError> {
	if args.len() != expected {
		return Err(RoutineError::FunctionArityMismatch {
			function: ctx.fragment.clone(),
			expected,
			actual: args.len(),
		});
	}
	Ok(())
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

pub fn dispatch_two<Op: ArithOp>(
	ctx: &mut FunctionContext,
	args: &Columns,
	strategy: BasicStrategy,
) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 2)?;
	execute_arith::<Op>(ctx, &args[0], &args[1], strategy.row_mode(), strategy.coerce_policy(), None, None)
}

pub fn dispatch_fallback<Op: ArithOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 3)?;
	let (d_data, _) = args[2].unwrap_option();
	ensure_numeric(ctx, d_data, 2)?;
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Fallback, CoercePolicy::Error, Some(&args[2]), None)
}

pub fn dispatch_strict<Op: ArithOp>(ctx: &mut FunctionContext, args: &Columns) -> Result<Columns, RoutineError> {
	ensure_arity(ctx, args, 3)?;
	let (msg_data, _) = args[2].unwrap_option();
	if msg_data.get_type() != ValueType::Utf8 {
		return Err(RoutineError::FunctionInvalidArgumentType {
			function: ctx.fragment.clone(),
			argument_index: 2,
			expected: vec![ValueType::Utf8],
			actual: msg_data.get_type(),
		});
	}
	execute_arith::<Op>(ctx, &args[0], &args[1], RowMode::Strict, CoercePolicy::Error, None, Some(msg_data))
}

fn execute_arith<Op: ArithOp>(
	ctx: &mut FunctionContext,
	a_col: &ColumnBuffer,
	b_col: &ColumnBuffer,
	mode: RowMode,
	policy: CoercePolicy,
	fallback_col: Option<&ColumnBuffer>,
	strict_msg: Option<&ColumnBuffer>,
) -> Result<Columns, RoutineError> {
	let (a_data, _) = a_col.unwrap_option();
	let (b_data, _) = b_col.unwrap_option();
	ensure_numeric(ctx, a_data, 0)?;
	ensure_numeric(ctx, b_data, 1)?;

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
	let a_cast = coerce_column(ctx, a_col, promoted.clone(), policy)?;
	let b_cast = coerce_column(ctx, b_col, promoted.clone(), policy)?;
	let d_cast = fallback_col.map(|d| coerce_column(ctx, d, promoted.clone(), CoercePolicy::Error)).transpose()?;

	let (a_inner, a_bv) = a_cast.unwrap_option();
	let (b_inner, b_bv) = b_cast.unwrap_option();
	let d_parts = d_cast.as_ref().map(|d| d.unwrap_option());

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
				(c, *bv)
			});
			compute_rows::<_, Op>(ctx, &promoted, (l, a_bv), (r, b_bv), &mode, d, strict_msg)?
		}};
		($container_variant:ident { .. }) => {{
			let (
				ColumnBuffer::$container_variant {
					container: l,
					..
				},
				ColumnBuffer::$container_variant {
					container: r,
					..
				},
			) = (a_inner, b_inner)
			else {
				unreachable!()
			};
			let d = d_parts.as_ref().map(|(inner, bv)| {
				let ColumnBuffer::$container_variant {
					container: c,
					..
				} = inner
				else {
					unreachable!()
				};
				(c, *bv)
			});
			compute_rows::<_, Op>(ctx, &promoted, (l, a_bv), (r, b_bv), &mode, d, strict_msg)?
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

fn compute_rows<T: SafeNum, Op: ArithOp>(
	ctx: &FunctionContext,
	promoted: &ValueType,
	l: (&NumberContainer<T>, Option<&BitVec>),
	r: (&NumberContainer<T>, Option<&BitVec>),
	mode: &RowMode,
	fallback: Option<(&NumberContainer<T>, Option<&BitVec>)>,
	strict_msg: Option<&ColumnBuffer>,
) -> Result<(Vec<T>, Vec<bool>), RoutineError> {
	fn defined<T: IsNumber>(c: &NumberContainer<T>, bv: Option<&BitVec>, i: usize) -> bool {
		c.is_defined(i) && bv.is_none_or(|b| b.get(i))
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

		let value = match mode {
			RowMode::Default => {
				if Op::DIVISIVE && SafeDiv::is_zero(rv) {
					return Err(TypeError::DivisionByZero {
						target: promoted.clone(),
						fragment: ctx.fragment.clone(),
					}
					.into());
				}
				match Op::checked(lv, rv) {
					Some(v) => v,
					None => {
						return Err(TypeError::NumberOutOfRange {
							target: promoted.clone(),
							fragment: ctx.fragment.clone(),
							descriptor: None,
						}
						.into());
					}
				}
			}
			RowMode::Strict => match Op::checked(lv, rv) {
				Some(v) => v,
				None => {
					return Err(make_strict_error(
						ctx,
						strict_msg.expect("strict mode carries a message column"),
						i,
					));
				}
			},
			RowMode::Saturate => Op::saturating(lv, rv),
			RowMode::Wrap => Op::wrapping(lv, rv),
			RowMode::Zero => Op::checked(lv, rv).unwrap_or_default(),
			RowMode::None => match Op::checked(lv, rv) {
				Some(v) => v,
				None => {
					values.push(T::default());
					bits.push(false);
					continue;
				}
			},
			RowMode::Fallback => match Op::checked(lv, rv) {
				Some(v) => v,
				None => {
					let (d, d_bv) = fallback.expect("fallback mode carries a fallback column");
					if defined(d, d_bv, i) {
						d.get(i).expect("defined row has a value").clone()
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
