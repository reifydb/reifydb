// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, push::Push};
use reifydb_value::{
	error::{BinaryOp, TypeError},
	fragment::LazyFragment,
	reifydb_assertions,
	value::{
		container::{
			bignum_array::{decimals, ints, uints},
			decimal_array::u128s,
		},
		is::IsNumber,
		number::{promote::Promote, safe::div::SafeDiv},
		value_type::{ValueType, get::GetType},
	},
};

use crate::{
	Result,
	expression::{context::EvalContext, option::arith_op_unwrap_option},
};

pub fn div_columns(
	ctx: &EvalContext,
	left: &ColumnWithName,
	right: &ColumnWithName,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName> {
	arith_op_unwrap_option(left, right, fragment.fragment(), |left, right| {
		let target = ValueType::promote(left.get_type(), right.get_type());

		dispatch_arith!(
			&left.data(), &right.data();
			fixed: div_numeric, arb: div_numeric_clone (ctx, target, fragment);

			_ => Err(TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Div,
				left: left.get_type(),
				right: right.get_type(),
				fragment: fragment.fragment(),
			}.into()),
		)
	})
}

fn div_numeric<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
where
	L: GetType + Promote<R> + IsNumber,
	R: GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeDiv,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		if let Some(value) = ctx.div(&l[i], &r[i], fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}

fn div_numeric_clone<L, R>(
	ctx: &EvalContext,
	l: &[L],
	r: &[R],
	target: ValueType,
	fragment: impl LazyFragment + Copy,
) -> Result<ColumnWithName>
where
	L: Clone + GetType + Promote<R> + IsNumber,
	R: Clone + GetType + IsNumber,
	<L as Promote<R>>::Output: IsNumber,
	<L as Promote<R>>::Output: SafeDiv,
	ColumnBuilder: Push<<L as Promote<R>>::Output>,
{
	reifydb_assertions! {
		assert_eq!(l.len(), r.len());
	}

	let mut data = ColumnBuilder::with_capacity(target, l.len());
	for i in 0..l.len() {
		let l_clone = l[i].clone();
		let r_clone = r[i].clone();
		if let Some(value) = ctx.div(&l_clone, &r_clone, fragment)? {
			data.push(value);
		} else {
			data.push_none()
		}
	}
	Ok(ColumnWithName {
		name: fragment.fragment(),
		data: data.finish(),
	})
}
