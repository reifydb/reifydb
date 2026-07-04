// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	error::TypeError,
	util::bitvec::BitVec,
	value::{
		container::number::NumberContainer, decimal::Decimal, int::Int, is::IsNumber, uint::Uint,
		value_type::ValueType,
	},
};

use crate::{
	function::{
		math::arith::dispatch::{ensure_arity, ensure_numeric},
		support::coerce::{CoercePolicy, all_rows_none, coerce_column, promote_pair},
	},
	routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError},
};

pub struct Power {
	info: RoutineInfo,
}

impl Default for Power {
	fn default() -> Self {
		Self::new()
	}
}

impl Power {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("math::power"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for Power {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		if input_types.len() >= 2 {
			promote_pair(input_types[0].clone(), input_types[1].clone())
		} else {
			ValueType::Float8
		}
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		ensure_arity(ctx, args, 2)?;

		let (base_data, _) = args[0].unwrap_option();
		let (exp_data, _) = args[1].unwrap_option();
		ensure_numeric(ctx, base_data, 0)?;
		ensure_numeric(ctx, exp_data, 1)?;

		let promoted = promote_pair(base_data.get_type(), exp_data.get_type());
		if promoted == ValueType::Any {
			if all_rows_none(&args[0]) && all_rows_none(&args[1]) {
				let result = ColumnBuffer::none_typed(ValueType::Any, base_data.len());
				return Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]));
			}
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![],
				actual: ValueType::Any,
			});
		}
		let base_cast = coerce_column(ctx, &args[0], promoted.clone(), CoercePolicy::Error)?;
		let exp_cast = coerce_column(ctx, &args[1], promoted.clone(), CoercePolicy::Error)?;

		let (base_inner, base_bv) = base_cast.unwrap_option();
		let (exp_inner, exp_bv) = exp_cast.unwrap_option();

		let overflow = || -> RoutineError {
			TypeError::NumberOutOfRange {
				target: promoted.clone(),
				fragment: ctx.fragment.clone(),
				descriptor: None,
			}
			.into()
		};

		macro_rules! run {
			($variant:ident, $factory:ident, $op:expr) => {{
				let (ColumnBuffer::$variant(b), ColumnBuffer::$variant(e)) = (base_inner, exp_inner)
				else {
					unreachable!()
				};
				let (values, bits) = pow_rows(b, base_bv, e, exp_bv, $op, &overflow)?;
				ColumnBuffer::$factory(values, bits)
			}};
			($variant:ident { .. }, $factory:ident, $op:expr) => {{
				let (
					ColumnBuffer::$variant {
						container: b,
						..
					},
					ColumnBuffer::$variant {
						container: e,
						..
					},
				) = (base_inner, exp_inner)
				else {
					unreachable!()
				};
				let (values, bits) = pow_rows(b, base_bv, e, exp_bv, $op, &overflow)?;
				ColumnBuffer::$factory(values, bits)
			}};
		}

		macro_rules! signed_pow_op {
			() => {
				|b, e| {
					if *e < 0 {
						Some(0)
					} else {
						u32::try_from(*e).ok().and_then(|exp| b.checked_pow(exp))
					}
				}
			};
		}

		macro_rules! unsigned_pow_op {
			() => {
				|b, e| u32::try_from(*e).ok().and_then(|exp| b.checked_pow(exp))
			};
		}

		let result = match promoted {
			ValueType::Int1 => run!(Int1, int1_with_bitvec, signed_pow_op!()),
			ValueType::Int2 => run!(Int2, int2_with_bitvec, signed_pow_op!()),
			ValueType::Int4 => run!(Int4, int4_with_bitvec, signed_pow_op!()),
			ValueType::Int8 => run!(Int8, int8_with_bitvec, signed_pow_op!()),
			ValueType::Int16 => run!(Int16, int16_with_bitvec, signed_pow_op!()),
			ValueType::Uint1 => run!(Uint1, uint1_with_bitvec, unsigned_pow_op!()),
			ValueType::Uint2 => run!(Uint2, uint2_with_bitvec, unsigned_pow_op!()),
			ValueType::Uint4 => run!(Uint4, uint4_with_bitvec, unsigned_pow_op!()),
			ValueType::Uint8 => run!(Uint8, uint8_with_bitvec, unsigned_pow_op!()),
			ValueType::Uint16 => run!(Uint16, uint16_with_bitvec, unsigned_pow_op!()),
			ValueType::Float4 => run!(Float4, float4_with_bitvec, |b: &f32, e: &f32| Some(b.powf(*e))),
			ValueType::Float8 => run!(Float8, float8_with_bitvec, |b: &f64, e: &f64| Some(b.powf(*e))),
			ValueType::Int => run!(Int { .. }, int_with_bitvec, |b: &Int, e: &Int| {
				if *e < Int::zero() {
					Some(Int::zero())
				} else {
					e.0.to_u32().map(|exp| Int::from(b.0.pow(exp)))
				}
			}),
			ValueType::Uint => run!(Uint { .. }, uint_with_bitvec, |b: &Uint, e: &Uint| {
				e.0.to_u32().map(|exp| Uint::from(b.0.pow(exp)))
			}),
			ValueType::Decimal => run!(Decimal { .. }, decimal_with_bitvec, |b: &Decimal, e: &Decimal| {
				let base = b.0.to_f64().unwrap_or(0.0);
				let exp = e.0.to_f64().unwrap_or(0.0);
				Some(Decimal::from(base.powf(exp)))
			}),
			_ => unreachable!("promotion of numeric inputs yields a numeric type"),
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
	}
}

fn pow_rows<T, F>(
	b: &NumberContainer<T>,
	b_bv: Option<&BitVec>,
	e: &NumberContainer<T>,
	e_bv: Option<&BitVec>,
	op: F,
	overflow: &dyn Fn() -> RoutineError,
) -> Result<(Vec<T>, Vec<bool>), RoutineError>
where
	T: IsNumber + Clone + Default,
	F: Fn(&T, &T) -> Option<T>,
{
	fn defined<T: IsNumber>(c: &NumberContainer<T>, bv: Option<&BitVec>, i: usize) -> bool {
		c.is_defined(i) && bv.is_none_or(|b| b.get(i))
	}

	let row_count = b.len();
	let mut values = Vec::with_capacity(row_count);
	let mut bits = Vec::with_capacity(row_count);

	for i in 0..row_count {
		if !defined(b, b_bv, i) || !defined(e, e_bv, i) {
			values.push(T::default());
			bits.push(false);
			continue;
		}
		let base = b.get(i).expect("defined row has a value");
		let exp = e.get(i).expect("defined row has a value");
		match op(base, exp) {
			Some(v) => {
				values.push(v);
				bits.push(true);
			}
			None => return Err(overflow()),
		}
	}

	Ok((values, bits))
}

impl Function for Power {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
